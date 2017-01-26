package mil.nga.giat.geowave.analytic.mapreduce.clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytic.Projection;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.SimpleFeatureProjection;
import mil.nga.giat.geowave.analytic.clustering.CentroidManager;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.mapreduce.GeoWaveWritableInputMapper;
import mil.nga.giat.geowave.mapreduce.GeoWaveWritableInputReducer;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.geotools.feature.type.BasicFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * Compute the convex hull over all points associated with each centroid. Each
 * hull is sent to output as a simple features.
 * 
 * Properties:
 * 
 * @formatter:off
 * 
 *                "ConvexHullMapReduce.Hull.DataTypeId" - Id of the data type to
 *                store the the polygons as simple features - defaults to
 *                "convex_hull"
 * 
 *                "ConvexHullMapReduce.Hull.ProjectionClass" - instance of
 *                {@link mil.nga.giat.geowave.analytic.Projection}
 * 
 *                "ConvexHullMapReduce.Hull.IndexId" - The Index ID used for
 *                output simple features.
 * 
 *                "ConvexHullMapReduce.Hull.WrapperFactoryClass" ->
 *                {@link AnalyticItemWrapperFactory} to group and level
 *                associated with each entry
 * 
 * @see mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment
 * 
 * @formatter:on
 */
public class ConvexHullMapReduce
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(ConvexHullMapReduce.class);

	public static class ConvexHullMap<T> extends
			GeoWaveWritableInputMapper<GeoWaveInputKey, ObjectWritable>
	{

		protected GeoWaveInputKey outputKey = new GeoWaveInputKey();
		private ObjectWritable currentValue;
		private AnalyticItemWrapperFactory<T> itemWrapperFactory;
		private NestedGroupCentroidAssignment<T> nestedGroupCentroidAssigner;

		// Override parent since there is not need to decode the value.
		@Override
		protected void mapWritableValue(
				final GeoWaveInputKey key,
				final ObjectWritable value,
				final Mapper<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
				throws IOException,
				InterruptedException {
			// cached for efficiency since the output is the input object
			// the de-serialized input object is only used for sampling.
			// For simplicity, allow the de-serialization to occur in all cases,
			// even though some sampling
			// functions do not inspect the input object.
			currentValue = value;
			super.mapWritableValue(
					key,
					value,
					context);
		}

		@Override
		protected void mapNativeValue(
				final GeoWaveInputKey key,
				final Object value,
				final org.apache.hadoop.mapreduce.Mapper<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
				throws IOException,
				InterruptedException {

			@SuppressWarnings("unchecked")
			final AnalyticItemWrapper<T> wrapper = itemWrapperFactory.create((T) value);
			outputKey.setAdapterId(key.getAdapterId());
			outputKey.setDataId(new ByteArrayId(
					StringUtils.stringToBinary(nestedGroupCentroidAssigner.getGroupForLevel(wrapper))));
			outputKey.setInsertionId(key.getInsertionId());
			context.write(
					outputKey,
					currentValue);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(
				final Mapper<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);

			final ScopedJobConfiguration config = new ScopedJobConfiguration(
					context.getConfiguration(),
					ConvexHullMapReduce.class,
					ConvexHullMapReduce.LOGGER);
			try {
				itemWrapperFactory = config.getInstance(
						HullParameters.Hull.WRAPPER_FACTORY_CLASS,
						AnalyticItemWrapperFactory.class,
						SimpleFeatureItemWrapperFactory.class);

				itemWrapperFactory.initialize(
						context,
						ConvexHullMapReduce.class,
						ConvexHullMapReduce.LOGGER);
			}
			catch (final Exception e1) {

				throw new IOException(
						e1);
			}

			try {
				nestedGroupCentroidAssigner = new NestedGroupCentroidAssignment<T>(
						context,
						ConvexHullMapReduce.class,
						ConvexHullMapReduce.LOGGER);
			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}
		}
	}

	public static class ConvexHullReducer<T> extends
			GeoWaveWritableInputReducer<GeoWaveOutputKey, SimpleFeature>
	{

		private CentroidManager<T> centroidManager;
		private List<ByteArrayId> indexIds;
		private FeatureDataAdapter outputAdapter;
		private Projection<T> projectionFunction;
		/*
		 * Logic inspired by SpatialHadoop convexHullStream method
		 */
		// absolute point cloud limit
		private final int pointCloudThreshold = 50000000;

		private final List<Coordinate> batchCoords = new ArrayList<Coordinate>(
				10000);

		@Override
		protected void reduceNativeValues(
				final GeoWaveInputKey key,
				final Iterable<Object> values,
				final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, SimpleFeature>.Context context )
				throws IOException,
				InterruptedException {
			// limit on new points per convex hull run (batch)
			int batchThreshold = 10000;

			batchCoords.clear();

			Geometry currentHull = null;

			final String groupID = StringUtils.stringFromBinary(key.getDataId().getBytes());
			final AnalyticItemWrapper<T> centroid = centroidManager.getCentroid(groupID);
			for (final Object value : values) {
				currentHull = null;
				@SuppressWarnings("unchecked")
				final Geometry geo = projectionFunction.getProjection((T) value);
				final Coordinate[] coords = geo.getCoordinates();
				if ((coords.length + batchCoords.size()) > pointCloudThreshold) {
					break;
				}
				for (final Coordinate coordinate : coords) {
					batchCoords.add(coordinate);
				}
				if (coords.length > batchThreshold) {
					batchThreshold = coords.length;
				}
				if (batchCoords.size() > batchThreshold) {
					currentHull = compress(
							key,
							batchCoords);
				}
			}
			currentHull = (currentHull == null) ? compress(
					key,
					batchCoords) : currentHull;

			if (ConvexHullMapReduce.LOGGER.isTraceEnabled()) {
				ConvexHullMapReduce.LOGGER.trace(centroid.getGroupID() + " contains " + groupID);
			}

			final SimpleFeature newPolygonFeature = AnalyticFeature.createGeometryFeature(
					outputAdapter.getFeatureType(),
					centroid.getBatchID(),
					UUID.randomUUID().toString(),
					centroid.getName(),
					centroid.getGroupID(),
					centroid.getCost(),
					currentHull,
					new String[0],
					new double[0],
					centroid.getZoomLevel(),
					centroid.getIterationID(),
					centroid.getAssociationCount());
			// new center
			context.write(
					new GeoWaveOutputKey(
							outputAdapter.getAdapterId(),
							indexIds),
					newPolygonFeature);
		}

		private static <T> Geometry compress(
				final GeoWaveInputKey key,
				final List<Coordinate> batchCoords ) {
			final Coordinate[] actualCoords = batchCoords.toArray(new Coordinate[batchCoords.size()]);

			// generate convex hull for current batch of points
			final ConvexHull convexHull = new ConvexHull(
					actualCoords,
					new GeometryFactory());
			final Geometry hullGeometry = convexHull.getConvexHull();

			final Coordinate[] hullCoords = hullGeometry.getCoordinates();
			batchCoords.clear();
			for (final Coordinate hullCoord : hullCoords) {
				batchCoords.add(hullCoord);
			}

			return hullGeometry;
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(
				final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, SimpleFeature>.Context context )
				throws IOException,
				InterruptedException {

			final ScopedJobConfiguration config = new ScopedJobConfiguration(
					context.getConfiguration(),
					ConvexHullMapReduce.class,
					ConvexHullMapReduce.LOGGER);
			super.setup(context);
			try {
				centroidManager = new CentroidManagerGeoWave<T>(
						context,
						ConvexHullMapReduce.class,
						ConvexHullMapReduce.LOGGER);
			}
			catch (final Exception e) {
				ConvexHullMapReduce.LOGGER.warn(
						"Unable to initialize centroid manager",
						e);
				throw new IOException(
						"Unable to initialize centroid manager");
			}

			try {
				projectionFunction = config.getInstance(
						HullParameters.Hull.PROJECTION_CLASS,
						Projection.class,
						SimpleFeatureProjection.class);

				projectionFunction.initialize(
						context,
						ConvexHullMapReduce.class);
			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}

			final String polygonDataTypeId = config.getString(
					HullParameters.Hull.DATA_TYPE_ID,
					"convex_hull");

			outputAdapter = AnalyticFeature.createGeometryFeatureAdapter(
					polygonDataTypeId,
					new String[0],
					config.getString(
							HullParameters.Hull.DATA_NAMESPACE_URI,
							BasicFeatureTypes.DEFAULT_NAMESPACE),
					ClusteringUtils.CLUSTERING_CRS);

			indexIds = new ArrayList<ByteArrayId>();
			indexIds.add(new ByteArrayId(
					StringUtils.stringToBinary(config.getString(
							HullParameters.Hull.INDEX_ID,
							new SpatialDimensionalityTypeProvider.SpatialIndexBuilder()
									.createIndex()
									.getId()
									.getString()))));

		}
	}

}
