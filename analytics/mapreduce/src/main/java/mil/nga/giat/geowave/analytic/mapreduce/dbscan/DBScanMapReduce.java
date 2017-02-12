package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.FeatureWritable;
import mil.nga.giat.geowave.analytic.AdapterWithObjectWritable;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.Projection;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.SimpleFeatureProjection;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.distance.CoordinateCircleDistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.dbscan.ClusterNeighborList.ClusterNeighborListFactory;
import mil.nga.giat.geowave.analytic.mapreduce.dbscan.ClusterUnionList.ClusterUnionListFactory;
import mil.nga.giat.geowave.analytic.mapreduce.dbscan.PreProcessSingleItemClusterList.PreProcessSingleItemClusterListFactory;
import mil.nga.giat.geowave.analytic.mapreduce.dbscan.SingleItemClusterList.SingleItemClusterListFactory;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNMapReduce;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNMapReduce.NNReducer;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NNMapReduce.PartitionDataWritable;
import mil.nga.giat.geowave.analytic.nn.NNProcessor;
import mil.nga.giat.geowave.analytic.nn.NNProcessor.CompleteNotifier;
import mil.nga.giat.geowave.analytic.nn.NeighborList;
import mil.nga.giat.geowave.analytic.nn.NeighborListFactory;
import mil.nga.giat.geowave.analytic.nn.TypeConverter;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner.PartitionData;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.mapreduce.HadoopWritableSerializer;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.geotools.feature.type.BasicFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The approach differs from the approach commonly documented (e.g.
 * https://en.wikipedia.org/wiki/DBSCAN). This approach does not maintain a
 * queue of viable neighbors to navigate.
 * 
 * Clusters are merged if they share neighbors in common and both clusters meet
 * the minimum size constraints.
 * 
 * Clusters may be made up of points or geometries. When processing geometries,
 * the closest two points are included in the cluster, not the entire geometry.
 * The reason for this is that geometries may span large areas. This technique
 * has a disadvantage of mis-representing dense segments as a dense set of
 * points.
 * 
 * The design uses two level partitioning, working within the confines of @{link
 * NNProcessor}. Performance gains and memory constraints are accomplished
 * through a pre-processing step.
 * 
 * Pre-processing first finds dense clusters, replacing each dense cluster with
 * a concave polygon. Although not very scientific, the condensing process the
 * minimum condensed cluster size is between 50 and 200, depending on the
 * setting of the minimum owners. The choice is some what arbitrary. Retaining
 * individual points for clusters larger than 200 often creates memory concerns.
 * However, there is little value in condensing below 50 as that indicates a
 * fairly small cluster, which does not contribute to a performance concern.
 * Override 'calculateCondensingMinimum ()' to come up with a different
 * approach.
 * 
 * Pre-processing also finds cluster centers that have less than the minimum and
 * tosses those centers. There is a caution here. Clusters of this type can fall
 * on the 'edge' of dense clusters, thus 'tightening' the dense regions. It does
 * effectively remove outliers. Alter the approach by over-riding
 * 'calculateTossMinimum()' (e.g. make it a smaller number like 0 or 1).
 * 
 * 
 */
public class DBScanMapReduce
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(DBScanMapReduce.class);

	public abstract static class DBScanMapReducer<KEYOUT, VALUEOUT> extends
			NNReducer<ClusterItem, KEYOUT, VALUEOUT, Map<ByteArrayId, Cluster>>
	{
		protected int minOwners = 0;

		@Override
		protected Map<ByteArrayId, Cluster> createSummary() {
			return new HashMap<ByteArrayId, Cluster>();
		}

		@Override
		protected void processNeighbors(
				final PartitionData partitionData,
				final ByteArrayId primaryId,
				final ClusterItem primary,
				final NeighborList<ClusterItem> neighbors,
				final Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT>.Context context,
				final Map<ByteArrayId, Cluster> index )
				throws IOException,
				InterruptedException {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace(
						"Finish {} ",
						primaryId);
			}
			if (neighbors == null) {
				return;
			}
			final Cluster cluster = ((ClusterNeighborList) neighbors).getCluster();
			if (cluster == null) {
				return;
			}
			if (cluster.size() < minOwners) {
				LOGGER.trace(
						"Invalidate {} ",
						primaryId);
				cluster.invalidate();
				return;
			}
			cluster.finish();
		}

		@Override
		protected void setup(
				final Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);
			final ScopedJobConfiguration config = new ScopedJobConfiguration(
					context.getConfiguration(),
					NNMapReduce.class);

			// first run must at least form a triangle
			minOwners = config.getInt(
					ClusteringParameters.Clustering.MINIMUM_SIZE,
					2);

			LOGGER.info(
					"Minumum owners = {}",
					minOwners);

		}

	}

	public static class SimpleFeatureToClusterItemConverter implements
			TypeConverter<ClusterItem>
	{

		final Projection<SimpleFeature> projection;

		public SimpleFeatureToClusterItemConverter(
				final Projection<SimpleFeature> projection ) {
			super();
			this.projection = projection;
		}

		@Override
		public ClusterItem convert(
				final ByteArrayId id,
				final Object o ) {
			final SimpleFeature feature = (SimpleFeature) o;
			final Long count = (Long) feature.getAttribute(AnalyticFeature.ClusterFeatureAttribute.COUNT.attrName());

			return new ClusterItem(
					feature.getID(),
					projection.getProjection(feature),
					count == null ? 1 : count,
					false);
		}
	}

	public static class DBScanMapHullReducer extends
			DBScanMapReducer<GeoWaveInputKey, ObjectWritable>
	{
		private String batchID;
		private int zoomLevel = 1;
		private int iteration = 1;
		private FeatureDataAdapter outputAdapter;

		private final ObjectWritable output = new ObjectWritable();
		private boolean firstIteration = true;

		protected int calculateCondensingMinimum() {
			return Math.min(
					Math.max(
							minOwners,
							200),
					minOwners * 10);
		}

		protected int calculateTossMinimum() {
			return (minOwners - 2);
		}

		/**
		 * Find the large clusters and condense them down. Find the points that
		 * are not reachable to viable clusters and remove them.
		 * 
		 * @throws InterruptedException
		 * @throws IOException
		 */
		@Override
		protected void preprocess(
				final Reducer<PartitionDataWritable, AdapterWithObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context,
				final NNProcessor<Object, ClusterItem> processor,
				final Map<ByteArrayId, Cluster> index )
				throws IOException,
				InterruptedException {
			if (!firstIteration) {
				return;
			}

			processor.trimSmallPartitions(calculateTossMinimum());
			// 2.0 times minimum compression size.
			// if compression is not likely to increase
			// performance, then pre-processing does not buy much performance
			if (processor.size() < (calculateCondensingMinimum() * 2.0)) {
				return;
			}

			processor.process(
					new ClusterNeighborListFactory(
							new PreProcessSingleItemClusterListFactory(
									index),
							index),
					new CompleteNotifier<ClusterItem>() {

						final int condenseSize = calculateCondensingMinimum();
						final int tossSize = calculateTossMinimum();

						@Override
						public void complete(
								final ByteArrayId id,
								final ClusterItem value,
								final NeighborList<ClusterItem> list ) {
							final Cluster cluster = ((ClusterNeighborList) list).getCluster();
							// this basically excludes points that cannot
							// contribute to extending the network.
							// may be a BAD idea.
							if (cluster.size() < tossSize) {
								processor.remove(id);
							}
							// this is a condensing component
							else if (cluster.size() > condenseSize) {
								cluster.finish();
								value.setGeometry(cluster.getGeometry());
								value.setCount(list.size());
								value.setCompressed();
								final Iterator<ByteArrayId> it = cluster.getLinkedClusters().iterator();
								while (it.hasNext()) {
									final ByteArrayId idToRemove = it.next();
									processor.remove(idToRemove);
									it.remove();
								}
							}
							else {
								cluster.clear();
							}
							context.progress();
						}
					});
			index.clear();
		}

		@Override
		protected void processSummary(
				final PartitionData partitionData,
				final Map<ByteArrayId, Cluster> summary,
				final Reducer<PartitionDataWritable, AdapterWithObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
				throws IOException,
				InterruptedException {
			final HadoopWritableSerializer<SimpleFeature, FeatureWritable> serializer = outputAdapter
					.createWritableSerializer();
			final Set<Cluster> processed = new HashSet<Cluster>();
			final Iterator<Map.Entry<ByteArrayId, Cluster>> clusterIt = summary.entrySet().iterator();
			while (clusterIt.hasNext()) {
				final Cluster cluster = clusterIt.next().getValue();
				clusterIt.remove();
				if (cluster.isCompressed() && !processed.contains(cluster)) {
					processed.add(cluster);
					final SimpleFeature newPolygonFeature = AnalyticFeature.createGeometryFeature(
							outputAdapter.getFeatureType(),
							batchID,
							UUID.randomUUID().toString(),
							cluster.getId().getString(), // name
							partitionData.getGroupId() != null ? partitionData.getGroupId().toString() : cluster
									.getId()
									.getString(), // group
							0.0,
							cluster.getGeometry(),
							new String[0],
							new double[0],
							zoomLevel,
							iteration,
							cluster.size());
					output.set(serializer.toWritable(newPolygonFeature));
					if (LOGGER.isTraceEnabled()) {
						LOGGER.trace(
								"Generating {}",
								newPolygonFeature.toString());
					}
					// ShapefileTool.writeShape(
					// cluster.getId().getString() + iteration,
					// new File(
					// "./target/testdb_" + cluster.getId().getString() +
					// iteration),
					// new Geometry[] {
					// (Geometry) cluster.get()
					// });
					context.write(
							new GeoWaveInputKey(
									outputAdapter.getAdapterId(),
									new ByteArrayId(
											newPolygonFeature.getID())),
							output);
				}
			}
		}

		@Override
		public NeighborListFactory<ClusterItem> createNeighborsListFactory(
				final Map<ByteArrayId, Cluster> summary ) {
			return new ClusterNeighborListFactory(
					(firstIteration) ? new SingleItemClusterListFactory(
							summary) : new ClusterUnionListFactory(
							summary),
					summary);

		}

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(
				final Reducer<PartitionDataWritable, AdapterWithObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
				throws IOException,
				InterruptedException {

			final ScopedJobConfiguration config = new ScopedJobConfiguration(
					context.getConfiguration(),
					NNMapReduce.class);

			super.setup(context);

			DBScanClusterList.getHullTool().setDistanceFnForCoordinate(
					new CoordinateCircleDistanceFn());
			DBScanClusterList.setMergeSize(minOwners);

			batchID = config.getString(
					GlobalParameters.Global.BATCH_ID,
					UUID.randomUUID().toString());

			zoomLevel = config.getInt(
					HullParameters.Hull.ZOOM_LEVEL,
					1);

			iteration = config.getInt(
					HullParameters.Hull.ITERATION,
					1);

			firstIteration = context.getConfiguration().getBoolean(
					"first.iteration",
					true);

			final String polygonDataTypeId = config.getString(
					HullParameters.Hull.DATA_TYPE_ID,
					"concave_hull");

			outputAdapter = AnalyticFeature.createGeometryFeatureAdapter(
					polygonDataTypeId,
					new String[0],
					config.getString(
							HullParameters.Hull.DATA_NAMESPACE_URI,
							BasicFeatureTypes.DEFAULT_NAMESPACE),
					ClusteringUtils.CLUSTERING_CRS);

			Projection<SimpleFeature> projectionFunction;
			try {
				projectionFunction = config.getInstance(
						HullParameters.Hull.PROJECTION_CLASS,
						Projection.class,
						SimpleFeatureProjection.class);
			}
			catch (InstantiationException | IllegalAccessException e) {
				throw new IOException(
						e);
			}

			super.typeConverter = new SimpleFeatureToClusterItemConverter(
					projectionFunction);

			distanceProfileFn = new ClusterItemDistanceFn();

			super.distanceFn = new ClusterItemDistanceFn();

		}
	}
}
