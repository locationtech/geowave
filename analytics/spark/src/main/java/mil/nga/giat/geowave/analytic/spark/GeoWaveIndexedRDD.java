package mil.nga.giat.geowave.analytic.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.BoundingBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
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

									Geometry geom = (Geometry) inputFeature.getDefaultGeometry();
									if (geom == null) {
										return result.iterator();
									}
									// Extract bounding box from input feature
									BoundingBox bounds = inputFeature.getBounds();
									NumericRange xRange = new NumericRange(
											bounds.getMinX() - bufferAmount,
											bounds.getMaxX() + bufferAmount);
									NumericRange yRange = new NumericRange(
											bounds.getMinY() - bufferAmount,
											bounds.getMaxY() + bufferAmount);

									if (bounds.isEmpty()) {
										Envelope internalEnvelope = geom.getEnvelopeInternal();
										xRange = new NumericRange(
												internalEnvelope.getMinX() - bufferAmount,
												internalEnvelope.getMaxX() + bufferAmount);
										yRange = new NumericRange(
												internalEnvelope.getMinY() - bufferAmount,
												internalEnvelope.getMaxY() + bufferAmount);

									}

									NumericData[] boundsRange = {
										xRange,
										yRange
									};

									// Convert the data to how the api expects
									// and index
									// using strategy above
									BasicNumericDataset convertedBounds = new BasicNumericDataset(
											boundsRange);
									InsertionIds insertIds = indexStrategy.value().getInsertionIds(
											convertedBounds);

									// Sometimes the result can span more than
									// one row/cell
									// of a tier
									// When we span more than one row each
									// individual get
									// added as a separate output pair
									// TODO should this use composite IDs or
									// just the sort
									// keys
									for (Iterator<ByteArrayId> iter = insertIds.getCompositeInsertionIds().iterator(); iter
											.hasNext();) {
										ByteArrayId id = iter.next();
										// Id decomposes to byte array of Tier,
										// Bin, SFC
										// (Hilbert in this case) id)
										// There may be value in decomposing the
										// id and
										// storing tier + sfcIndex as a tuple
										// key of the new
										// RDD
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
		return this.getIndexedGeometryRDD(0.0);
	}

	public JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> getIndexedGeometryRDD(
			double bufferAmount ) {
		verifyParameters();

		if (!geowaveRDD.isLoaded()) {
			LOGGER.error("Must provide a loaded RDD.");
			return null;
		}
		if (rawGeometryRDD == null) {
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> indexedData = geowaveRDD
					.getRawRDD()
					.flatMapToPair(
							new PairFlatMapFunction<Tuple2<GeoWaveInputKey, SimpleFeature>, ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>() {
								@Override
								public Iterator<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>> call(
										Tuple2<GeoWaveInputKey, SimpleFeature> t )
										throws Exception {

									// Flattened output array.
									List<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>> result = new ArrayList<>();

									// Pull feature to index from tuple
									SimpleFeature inputFeature = t._2;

									Geometry geom = (Geometry) inputFeature.getDefaultGeometry();
									if (geom == null) {
										return result.iterator();
									}
									// Extract bounding box from input feature
									BoundingBox bounds = inputFeature.getBounds();
									NumericRange xRange = new NumericRange(
											bounds.getMinX() - bufferAmount,
											bounds.getMaxX() + bufferAmount);
									NumericRange yRange = new NumericRange(
											bounds.getMinY() - bufferAmount,
											bounds.getMaxY() + bufferAmount);

									if (bounds.isEmpty()) {
										Envelope internalEnvelope = geom.getEnvelopeInternal();
										xRange = new NumericRange(
												internalEnvelope.getMinX() - bufferAmount,
												internalEnvelope.getMaxX() + bufferAmount);
										yRange = new NumericRange(
												internalEnvelope.getMinY() - bufferAmount,
												internalEnvelope.getMaxY() + bufferAmount);

									}

									NumericData[] boundsRange = {
										xRange,
										yRange
									};

									// Convert the data to how the api expects
									// and index
									// using strategy above
									BasicNumericDataset convertedBounds = new BasicNumericDataset(
											boundsRange);
									InsertionIds insertIds = indexStrategy.value().getInsertionIds(
											convertedBounds);

									// Sometimes the result can span more than
									// one row/cell
									// of a tier
									// When we span more than one row each
									// individual get
									// added as a separate output pair
									// TODO should this use composite IDs or
									// just the sort
									// keys
									for (Iterator<ByteArrayId> iter = insertIds.getCompositeInsertionIds().iterator(); iter
											.hasNext();) {
										ByteArrayId id = iter.next();
										// Id decomposes to byte array of Tier,
										// Bin, SFC
										// (Hilbert in this case) id)
										// There may be value in decomposing the
										// id and
										// storing tier + sfcIndex as a tuple
										// key of the new
										// RDD
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
			rawGeometryRDD = indexedData;
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
