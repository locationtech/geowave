package mil.nga.giat.geowave.analytic.javaspark;

import java.io.IOException;
import java.util.Date;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.type.BasicFeatureTypes;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.adapter.vector.utils.DateUtilities;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.store.query.ScaledTemporalRange;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalRange;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import scala.Tuple2;

public class KMeansUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(KMeansUtils.class);

	public static DataAdapter writeClusterCentroids(
			final KMeansModel clusterModel,
			final DataStorePluginOptions outputDataStore,
			final String centroidAdapterName,
			final ScaledTemporalRange scaledRange ) {
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName(centroidAdapterName);
		typeBuilder.setNamespaceURI(BasicFeatureTypes.DEFAULT_NAMESPACE);

		try {
			typeBuilder.setCRS(CRS.decode(
					"EPSG:4326",
					true));
		}
		catch (final FactoryException fex) {
			LOGGER.error(
					fex.getMessage(),
					fex);
		}

		final AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();

		typeBuilder.add(attrBuilder.binding(
				Geometry.class).nillable(
				false).buildDescriptor(
				Geometry.class.getName().toString()));

		if (scaledRange != null) {
			typeBuilder.add(attrBuilder.binding(
					Date.class).nillable(
					false).buildDescriptor(
					"Time"));
		}

		typeBuilder.add(attrBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"KMeansData"));

		final SimpleFeatureType sfType = typeBuilder.buildFeatureType();
		final SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(
				sfType);

		final FeatureDataAdapter featureAdapter = new FeatureDataAdapter(
				sfType);

		final DataStore featureStore = outputDataStore.createDataStore();
		final PrimaryIndex featureIndex = new SpatialDimensionalityTypeProvider().createPrimaryIndex();

		try (IndexWriter writer = featureStore.createWriter(
				featureAdapter,
				featureIndex)) {
			int i = 0;
			for (final Vector center : clusterModel.clusterCenters()) {
				final double lon = center.apply(0);
				final double lat = center.apply(1);

				sfBuilder.set(
						Geometry.class.getName(),
						GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								lon,
								lat)));

				if (scaledRange != null && center.size() > 2) {
					final double timeVal = center.apply(2);

					Date time = scaledRange.valueToTime(timeVal);

					sfBuilder.set(
							"Time",
							time);

					LOGGER.warn("Write time: " + time);
				}

				sfBuilder.set(
						"KMeansData",
						"KMeansCentroid");

				final SimpleFeature sf = sfBuilder.buildFeature("Centroid-" + i++);

				writer.write(sf);
			}
		}
		catch (final MismatchedIndexToAdapterMapping e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}
		catch (final IOException e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}

		return featureAdapter;
	}

	public static DataAdapter writeClusterHulls(
			final JavaRDD<Vector> inputCentroids,
			final KMeansModel clusterModel,
			final DataStorePluginOptions outputDataStore,
			final String hullAdapterName ) {
		final JavaPairRDD<Integer, Geometry> hullRdd = KMeansHullGenerator.generateHullsRDD(
				inputCentroids,
				clusterModel);

		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName(hullAdapterName);
		typeBuilder.setNamespaceURI(BasicFeatureTypes.DEFAULT_NAMESPACE);
		try {
			typeBuilder.setCRS(CRS.decode(
					"EPSG:4326",
					true));
		}
		catch (final FactoryException e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}

		final AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();

		typeBuilder.add(attrBuilder.binding(
				Geometry.class).nillable(
				false).buildDescriptor(
				Geometry.class.getName().toString()));

		typeBuilder.add(attrBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"KMeansData"));

		final SimpleFeatureType sfType = typeBuilder.buildFeatureType();
		final SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(
				sfType);

		final FeatureDataAdapter featureAdapter = new FeatureDataAdapter(
				sfType);

		final DataStore featureStore = outputDataStore.createDataStore();
		final PrimaryIndex featureIndex = new SpatialDimensionalityTypeProvider().createPrimaryIndex();

		try (IndexWriter writer = featureStore.createWriter(
				featureAdapter,
				featureIndex)) {

			int i = 0;
			for (final Tuple2<Integer, Geometry> hull : hullRdd.collect()) {
				sfBuilder.set(
						Geometry.class.getName(),
						hull._2);

				sfBuilder.set(
						"KMeansData",
						"KMeansHull");

				final SimpleFeature sf = sfBuilder.buildFeature("Hull-" + i++);

				writer.write(sf);
			}
		}
		catch (final MismatchedIndexToAdapterMapping e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}
		catch (final IOException e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}

		return featureAdapter;
	}

	public static ScaledTemporalRange setRunnerTimeParams(
			final KMeansRunner runner,
			final DataStorePluginOptions inputDataStore,
			ByteArrayId adapterId ) {
		if (adapterId == null) { // locate the first feature adapter
			adapterId = FeatureDataUtils.getFirstFeatureAdapter(inputDataStore);
		}

		if (adapterId != null) {
			ScaledTemporalRange scaledRange = new ScaledTemporalRange();
			String timeField = FeatureDataUtils.getFirstTimeField(
					inputDataStore,
					adapterId);

			if (timeField != null) {
				TemporalRange timeRange = DateUtilities.getTemporalRange(
						inputDataStore,
						adapterId);

				if (timeRange != null) {
					scaledRange.setTimeRange(
							timeRange.getStartTime(),
							timeRange.getEndTime());
				}

				Envelope bbox = mil.nga.giat.geowave.adapter.vector.utils.GeometryUtils.getGeoBounds(
						inputDataStore,
						adapterId);

				if (bbox != null) {
					double xRange = bbox.getMaxX() - bbox.getMinX();
					double yRange = bbox.getMaxY() - bbox.getMinY();
					double valueRange = Math.min(
							xRange,
							yRange);
					scaledRange.setValueRange(
							0.0,
							valueRange);
				}

				runner.setTimeParams(
						timeField,
						scaledRange);

				return scaledRange;
			}
		}

		return null;
	}
}
