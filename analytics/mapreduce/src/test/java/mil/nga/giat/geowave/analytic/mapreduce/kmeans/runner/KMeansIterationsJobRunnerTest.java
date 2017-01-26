package mil.nga.giat.geowave.analytic.mapreduce.kmeans.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.CentroidManager;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.clustering.exception.MatchingCentroidNotFoundException;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class KMeansIterationsJobRunnerTest
{

	private final KMeansIterationsJobRunnerForTest jobRunner = new KMeansIterationsJobRunnerForTest();
	private static final String[] grps = new String[] {
		"g1",
		"g2"
	};
	private static final FeatureDataAdapter adapter = AnalyticFeature.createGeometryFeatureAdapter(
			"centroid",
			new String[] {},
			BasicFeatureTypes.DEFAULT_NAMESPACE,
			ClusteringUtils.CLUSTERING_CRS);

	PropertyManagement propertyMgt = new PropertyManagement();

	@Before
	public void setup() {
		propertyMgt.store(
				GlobalParameters.Global.BATCH_ID,
				"b1");
		propertyMgt.store(
				CentroidParameters.Centroid.DATA_TYPE_ID,
				"centroid");
		propertyMgt.store(
				CentroidParameters.Centroid.INDEX_ID,
				new SpatialDimensionalityTypeProvider().createPrimaryIndex().getId().getString());
		propertyMgt.store(
				ClusteringParameters.Clustering.CONVERGANCE_TOLERANCE,
				new Double(
						0.0001));
		propertyMgt.store(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				FeatureCentroidDistanceFn.class);
		propertyMgt.store(
				CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
				SimpleFeatureItemWrapperFactory.class);
	}

	@Test
	public void testRun()
			throws Exception {
		// seed
		jobRunner.runJob(
				new Configuration(),
				propertyMgt);
		// then test
		jobRunner.run(
				new Configuration(),
				propertyMgt);

		for (final Map.Entry<String, List<AnalyticItemWrapper<SimpleFeature>>> e : KMeansIterationsJobRunnerForTest.groups
				.entrySet()) {
			assertEquals(
					3,
					e.getValue().size());

			for (final AnalyticItemWrapper<SimpleFeature> newCentroid : e.getValue()) {
				assertEquals(
						2,
						newCentroid.getIterationID());
				// check to make sure there is no overlap of old and new IDs
				boolean b = false;
				for (final AnalyticItemWrapper<SimpleFeature> oldCentroid : KMeansIterationsJobRunnerForTest.deletedSet
						.get(e.getKey())) {
					b |= oldCentroid.getID().equals(
							newCentroid.getID());

				}
				assertFalse(b);
			}

		}

		for (final Map.Entry<String, List<AnalyticItemWrapper<SimpleFeature>>> e : KMeansIterationsJobRunnerForTest.deletedSet
				.entrySet()) {
			assertEquals(
					3,
					e.getValue().size());
			for (final AnalyticItemWrapper<SimpleFeature> oldCentroid : e.getValue()) {
				assertEquals(
						1,
						oldCentroid.getIterationID());
			}
		}

	}

	public static class KMeansIterationsJobRunnerForTest extends
			KMeansIterationsJobRunner<SimpleFeature>
	{
		private int iteration = 1;
		protected static Map<String, List<AnalyticItemWrapper<SimpleFeature>>> groups = new HashMap<String, List<AnalyticItemWrapper<SimpleFeature>>>();
		protected static Map<String, List<AnalyticItemWrapper<SimpleFeature>>> deletedSet = new HashMap<String, List<AnalyticItemWrapper<SimpleFeature>>>();
		private static SimpleFeatureItemWrapperFactory factory = new SimpleFeatureItemWrapperFactory();
		private static final GeometryFactory geoFactory = new GeometryFactory();
		private static Point[] points = new Point[] {
			geoFactory.createPoint(new Coordinate(
					2.3,
					2.3)),
			geoFactory.createPoint(new Coordinate(
					2.31,
					2.31)),
			geoFactory.createPoint(new Coordinate(
					2.32,
					2.31)),
			geoFactory.createPoint(new Coordinate(
					2.31,
					2.33)),
			geoFactory.createPoint(new Coordinate(
					2.29,
					2.31)),
			geoFactory.createPoint(new Coordinate(
					2.3,
					2.32)),
			geoFactory.createPoint(new Coordinate(
					2.28,
					2.3)),
			geoFactory.createPoint(new Coordinate(
					2.28,
					2.27)),
			geoFactory.createPoint(new Coordinate(
					2.27,
					2.31)),
			geoFactory.createPoint(new Coordinate(
					2.33,
					2.3)),
			geoFactory.createPoint(new Coordinate(
					2.31,
					2.35))
		};

		@Override
		protected CentroidManager<SimpleFeature> constructCentroidManager(
				final Configuration config,
				final PropertyManagement runTimeProperties )
				throws IOException {
			return new CentroidManager<SimpleFeature>() {

				@Override
				public void clear() {

				}

				@Override
				public AnalyticItemWrapper<SimpleFeature> createNextCentroid(
						final SimpleFeature feature,
						final String groupID,
						final Coordinate coordinate,
						final String[] extraNames,
						final double[] extraValues ) {
					return factory.createNextItem(
							feature,
							groupID,
							coordinate,
							extraNames,
							extraValues);
				}

				@Override
				public void delete(
						final String[] dataIds )
						throws IOException {
					final List<String> grps = Arrays.asList(dataIds);
					for (final Map.Entry<String, List<AnalyticItemWrapper<SimpleFeature>>> entry : groups.entrySet()) {
						final Iterator<AnalyticItemWrapper<SimpleFeature>> it = entry.getValue().iterator();
						while (it.hasNext()) {
							final AnalyticItemWrapper<SimpleFeature> next = it.next();
							if (grps.contains(next.getID())) {
								deletedSet.get(
										entry.getKey()).add(
										next);
								it.remove();
							}
						}
					}
				}

				@Override
				public List<String> getAllCentroidGroups()
						throws IOException {
					final List<String> ll = new ArrayList<String>();
					for (final String g : groups.keySet()) {
						ll.add(g);
					}
					return ll;
				}

				@Override
				public List<AnalyticItemWrapper<SimpleFeature>> getCentroidsForGroup(
						final String groupID )
						throws IOException {
					return groups.get(groupID);
				}

				@Override
				public List<AnalyticItemWrapper<SimpleFeature>> getCentroidsForGroup(
						final String batchID,
						final String groupID )
						throws IOException {
					return groups.get(groupID);
				}

				@Override
				public int processForAllGroups(
						final mil.nga.giat.geowave.analytic.clustering.CentroidManager.CentroidProcessingFn<SimpleFeature> fn )
						throws IOException {
					for (final Map.Entry<String, List<AnalyticItemWrapper<SimpleFeature>>> entry : groups.entrySet()) {
						final int status = fn.processGroup(
								entry.getKey(),
								entry.getValue());
						if (status < 0) {
							return status;
						}
					}
					return 0;
				}

				@Override
				public AnalyticItemWrapper<SimpleFeature> getCentroid(
						final String id ) {
					// TODO Auto-generated method stub
					return null;
				}

				@Override
				public ByteArrayId getDataTypeId() {
					return new ByteArrayId(
							StringUtils.stringToBinary("centroid"));
				}

				@Override
				public ByteArrayId getIndexId() {
					return new SpatialDimensionalityTypeProvider().createPrimaryIndex().getId();
				}

				@Override
				public AnalyticItemWrapper<SimpleFeature> getCentroidById(
						final String id,
						final String groupID )
						throws IOException,
						MatchingCentroidNotFoundException {
					final Iterator<AnalyticItemWrapper<SimpleFeature>> it = this.getCentroidsForGroup(
							groupID).iterator();
					while (it.hasNext()) {
						final AnalyticItemWrapper<SimpleFeature> feature = (it.next());
						if (feature.getID().equals(
								id)) {
							return feature;
						}
					}
					throw new MatchingCentroidNotFoundException(
							id);
				}

			};
		}

		@Override
		protected int runJob(
				final Configuration config,
				final PropertyManagement runTimeProperties )
				throws Exception {
			int j = 0;
			for (final String grpID : grps) {
				if (!groups.containsKey(grpID)) {
					groups.put(
							grpID,
							new ArrayList<AnalyticItemWrapper<SimpleFeature>>());
					deletedSet.put(
							grpID,
							new ArrayList<AnalyticItemWrapper<SimpleFeature>>());
				}
				for (int i = 0; i < 3; i++) {
					final SimpleFeature nextFeature = AnalyticFeature.createGeometryFeature(
							adapter.getFeatureType(),
							"b1",
							UUID.randomUUID().toString(),
							"nn" + i,
							grpID,
							0.1,
							points[j++],
							new String[0],
							new double[0],
							1,
							iteration,
							0);
					groups.get(
							grpID).add(
							factory.create(nextFeature));
				}
			}
			iteration++;
			return 0;
		}
	}

}
