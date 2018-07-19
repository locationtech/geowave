package mil.nga.giat.geowave.analytic.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import scala.Tuple2;

public class GeoWaveIndexedRDD implements
		Serializable
{

	private static Logger LOGGER = LoggerFactory.getLogger(GeoWaveIndexedRDD.class);
	private final GeoWaveRDD geowaveRDD;
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, SimpleFeature>> rawFeatureRDD = null;
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> rawGeometryRDD = null;
	// Because it can be expensive to serialize IndexStrategy for every record.
	// Index strategy must be able to be broadcast.
	private Broadcast<NumericIndexStrategy> indexStrategy = null;

	public GeoWaveIndexedRDD(
			final GeoWaveRDD geowaveRDD,
			final Broadcast<NumericIndexStrategy> indexStrategy ) {
		this.geowaveRDD = geowaveRDD;
		this.indexStrategy = indexStrategy;
	}

	public void reset() {
		this.rawFeatureRDD = null;
		this.rawGeometryRDD = null;
	}

	public void reindex(
			final Broadcast<? extends NumericIndexStrategy> newIndexStrategy ) {
		// Remove original indexing strategy
		if (this.indexStrategy != null) {
			this.indexStrategy.unpersist();
		}
		this.indexStrategy = (Broadcast<NumericIndexStrategy>) newIndexStrategy;
		reset();
	}

	public JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, SimpleFeature>> getIndexedFeatureRDD() {
		return this.getIndexedFeatureRDD(0.0);
	}

	public JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, SimpleFeature>> getIndexedFeatureRDD(
			double bufferAmount ) {
		verifyParameters();
		if (!geowaveRDD.isLoaded()) {
			LOGGER.error("Must provide a loaded RDD.");
			return null;
		}
		if (rawFeatureRDD == null) {
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, SimpleFeature>> indexedData = geowaveRDD
					.getRawRDD()
					.flatMapToPair(
							new PairFlatMapFunction<Tuple2<GeoWaveInputKey, SimpleFeature>, ByteArrayId, Tuple2<GeoWaveInputKey, SimpleFeature>>() {
								@Override
								public Iterator<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, SimpleFeature>>> call(
										Tuple2<GeoWaveInputKey, SimpleFeature> t )
										throws Exception {

									// Flattened output array.
									List<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, SimpleFeature>>> result = new ArrayList<>();

									// Pull feature to index from tuple
									SimpleFeature inputFeature = t._2;
									// If we are dealing with null or empty
									// geometry we can't properly compare this
									// feature.
									Geometry geom = (Geometry) inputFeature.getDefaultGeometry();
									if (geom == null) {
										return Iterators.emptyIterator();
									}

									Envelope internalEnvelope = geom.getEnvelopeInternal();
									if (internalEnvelope.isNull()) {
										return Iterators.emptyIterator();
									}
									// If we have to buffer geometry for
									// predicate expand bounds
									internalEnvelope.expandBy(bufferAmount);

									// Get data range from expanded envelope
									MultiDimensionalNumericData boundsRange = GeometryUtils
											.getBoundsFromEnvelope(internalEnvelope);

									NumericIndexStrategy index = indexStrategy.value();
									InsertionIds insertIds = index.getInsertionIds(
											boundsRange,
											80);

									// If we didnt expand the envelope for
									// buffering we can trim the indexIds by the
									// geometry
									if (bufferAmount == 0.0) {
										insertIds = RDDUtils.trimIndexIds(
												insertIds,
												geom,
												index);
									}

									for (Iterator<ByteArrayId> iter = insertIds.getCompositeInsertionIds().iterator(); iter
											.hasNext();) {
										ByteArrayId id = iter.next();

										Tuple2<GeoWaveInputKey, SimpleFeature> valuePair = new Tuple2<>(
												t._1,
												inputFeature);
										Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, SimpleFeature>> indexPair = new Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, SimpleFeature>>(
												id,
												valuePair);
										result.add(indexPair);
									}

									return result.iterator();
								}

							});
			rawFeatureRDD = indexedData;
		}

		return rawFeatureRDD;
	}

	public JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> getIndexedGeometryRDD() {
		return this.getIndexedGeometryRDD(
				0.0,
				false);
	}

	public JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> getIndexedGeometryRDD(
			double bufferAmount,
			boolean recalculate ) {
		verifyParameters();

		if (!geowaveRDD.isLoaded()) {
			LOGGER.error("Must provide a loaded RDD.");
			return null;
		}
		if (rawGeometryRDD == null || recalculate) {
			rawGeometryRDD = geowaveRDD
					.getRawRDD()
					.filter(t -> (t._2.getDefaultGeometry() != null && !((Geometry)t._2.getDefaultGeometry()).getEnvelopeInternal().isNull()))
					.flatMapToPair(
							new PairFlatMapFunction<Tuple2<GeoWaveInputKey, SimpleFeature>, ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>() {
								@Override
								public Iterator<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>> call(
										Tuple2<GeoWaveInputKey, SimpleFeature> t )
										throws Exception {

									// Pull feature to index from tuple
									SimpleFeature inputFeature = t._2;
									// If we are dealing with null or empty
									// geometry we can't properly compare this
									// feature.
									Geometry geom = (Geometry) inputFeature.getDefaultGeometry();

									Envelope internalEnvelope = geom.getEnvelopeInternal();
									// If we have to buffer geometry for
									// predicate expand bounds
									internalEnvelope.expandBy(bufferAmount);

									// Get data range from expanded envelope
									MultiDimensionalNumericData boundsRange = GeometryUtils
											.getBoundsFromEnvelope(internalEnvelope);

									NumericIndexStrategy index = indexStrategy.value();
									InsertionIds insertIds = index.getInsertionIds(
											boundsRange,
											80);

									// If we didnt expand the envelope for
									// buffering we can trim the indexIds by the
									// geometry
									if (bufferAmount == 0.0) {
										insertIds = RDDUtils.trimIndexIds(
												insertIds,
												geom,
												index);
									}

									// Flattened output array.
									List<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>> result = Lists
											.newArrayListWithCapacity(insertIds.getSize());

									for (Iterator<ByteArrayId> iter = insertIds.getCompositeInsertionIds().iterator(); iter
											.hasNext();) {
										ByteArrayId id = iter.next();

										Tuple2<GeoWaveInputKey, Geometry> valuePair = new Tuple2<>(
												t._1,
												geom);
										Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> indexPair = new Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>(
												id,
												valuePair);
										result.add(indexPair);
									}

									return result.iterator();
								}

							});
		}

		return rawGeometryRDD;
	}

	public Broadcast<NumericIndexStrategy> getIndexStrategy() {
		return indexStrategy;
	}

	public GeoWaveRDD getGeoWaveRDD() {
		return geowaveRDD;
	}

	private boolean verifyParameters() {
		if (geowaveRDD == null) {
			LOGGER.error("Must supply a input rdd to index. Please set one and try again.");
			return false;
		}
		if (indexStrategy == null) {
			LOGGER.error("Broadcasted strategy must be set before features can be indexed.");
			return false;
		}
		return true;
	}

}
