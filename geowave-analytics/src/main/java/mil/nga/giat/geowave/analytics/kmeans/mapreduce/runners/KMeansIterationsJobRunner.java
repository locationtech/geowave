package mil.nga.giat.geowave.analytics.kmeans.mapreduce.runners;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import mil.nga.giat.geowave.analytics.clustering.CentroidManager;
import mil.nga.giat.geowave.analytics.clustering.CentroidManager.CentroidProcessingFn;
import mil.nga.giat.geowave.analytics.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytics.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytics.distance.DistanceFn;
import mil.nga.giat.geowave.analytics.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytics.extract.SimpleFeatureCentroidExtractor;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.ClusteringParameters;
import mil.nga.giat.geowave.analytics.parameters.CommonParameters;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytics.tools.IndependentJobRunner;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobController;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * 
 * Run 'K' means until convergence across ALL groups.
 * 
 * 
 */
public class KMeansIterationsJobRunner<T> implements
		MapReduceJobRunner,
		IndependentJobRunner
{
	protected static final Logger LOGGER = Logger.getLogger(KMeansIterationsJobRunner.class);

	private final KMeansJobRunner jobRunner = new KMeansJobRunner();
	private double convergenceTol = 0.0001;

	public KMeansIterationsJobRunner() {}

	protected CentroidManager<T> constructCentroidManager(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws IOException {
		try {
			return new CentroidManagerGeoWave<T>(
					runTimeProperties);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			throw new IOException(
					e);
		}
	}

	public void setReducerCount(
			final int reducerCount ) {
		jobRunner.setReducerCount(reducerCount);
	}

	@SuppressWarnings("unchecked")
	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		convergenceTol = runTimeProperties.getPropertyAsDouble(
				ClusteringParameters.Clustering.CONVERGANCE_TOLERANCE,
				convergenceTol);

		final DistanceFn<T> distanceFunction = (DistanceFn<T>) runTimeProperties.getClassInstance(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				FeatureCentroidDistanceFn.class);

		int maxIterationCount = runTimeProperties.getPropertyAsInt(
				ClusteringParameters.Clustering.MAX_ITERATIONS,
				15);
		boolean converged = false;

		while (!converged && (maxIterationCount > 0)) {
			final int status = runJob(
					config,
					runTimeProperties);
			if (status != 0) {
				return status;
			}

			// new one each time to force a refresh if the centroids
			final CentroidManager<T> centroidManager = constructCentroidManager(
					config,
					runTimeProperties);

			// check for convergence
			converged = checkForConvergence(
					centroidManager,
					distanceFunction);

			maxIterationCount--;
		}
		return 0;
	}

	protected int runJob(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {
		jobRunner.setInputHDFSPath(runTimeProperties.getPropertyAsPath(CommonParameters.Common.HDFS_INPUT_PATH));

		runTimeProperties.storeIfEmpty(
				CentroidParameters.Centroid.EXTRACTOR_CLASS,
				SimpleFeatureCentroidExtractor.class);
		runTimeProperties.storeIfEmpty(
				CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
				SimpleFeatureItemWrapperFactory.class);
		runTimeProperties.storeIfEmpty(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				FeatureCentroidDistanceFn.class);
		return jobRunner.run(
				config,
				runTimeProperties);
	}

	private boolean checkForConvergence(
			final CentroidManager<T> centroidManager,
			final DistanceFn<T> distanceFunction )
			throws Exception {
		final AtomicInteger grpCount = new AtomicInteger(
				0);
		final AtomicInteger failuresCount = new AtomicInteger(
				0);
		final AtomicInteger centroidCount = new AtomicInteger(
				0);
		final boolean status = centroidManager.processForAllGroups(new CentroidProcessingFn<T>() {
			@Override
			public int processGroup(
					final String groupID,
					final List<AnalyticItemWrapper<T>> centroids ) {
				grpCount.incrementAndGet();
				centroidCount.addAndGet(centroids.size() / 2);

				failuresCount.addAndGet(computeCostAndCleanUp(
						groupID,
						centroids,
						centroidManager,
						distanceFunction));
				return 0;
			}

		}) == 0 ? true : false;
		// update default based on data size
		setReducerCount(grpCount.get() * centroidCount.get());
		return status && (failuresCount.get() == 0);
	}

	protected int computeCostAndCleanUp(
			final String groupID,
			final List<AnalyticItemWrapper<T>> centroids,
			final CentroidManager<T> centroidManager,
			final DistanceFn<T> distanceFunction ) {
		double distance = 0;
		final List<String> deletionKeys = new ArrayList<String>();

		// sort by id and then by iteration
		Collections.sort(
				centroids,
				new Comparator<AnalyticItemWrapper<T>>() {

					@Override
					public int compare(
							final AnalyticItemWrapper<T> arg0,
							final AnalyticItemWrapper<T> arg1 ) {
						final int c = arg0.getName().compareTo(
								arg1.getName());
						if (c == 0) {
							return arg0.getIterationID() - arg1.getIterationID();
						}
						else {
							return c;
						}
					}
				});
		AnalyticItemWrapper<T> prior = null;
		for (final AnalyticItemWrapper<T> centroid : centroids) {
			if (prior == null) {
				prior = centroid;
				continue;
			}
			else if (!prior.getName().equals(
					centroid.getName())) {
				// should we delete this...it is a centroid without assigned
				// points? This occurs when the number of centroids exceeds the
				// number of points in a cluster.
				// it is an edge case.
				// deletionKeys.add( prior.getID() );
				LOGGER.warn("Centroid is no longer viable " + prior.getName() + " from group " + prior.getGroupID());
				prior = centroid;
				continue;
			}
			// the prior run centroids are still present from the geowave data
			// store;
			// their priors do not exist in the map
			distance += distanceFunction.measure(
					prior.getWrappedItem(),
					centroid.getWrappedItem());
			deletionKeys.add(prior.getID());
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("Within group " + prior.getGroupID() + " replace " + prior.getID() + " with " + centroid.getID());
			}
			prior = null;
		}
		distance /= centroids.size();

		try {
			centroidManager.delete(deletionKeys.toArray(new String[deletionKeys.size()]));
		}
		catch (final Exception e) {
			throw new RuntimeException(
					e);
		}

		return (distance < convergenceTol) ? 0 : 1;
	}

	@Override
	public void fillOptions(
			final Set<Option> options ) {
		CentroidParameters.fillOptions(
				options,
				new CentroidParameters.Centroid[] {
					CentroidParameters.Centroid.INDEX_ID,
					CentroidParameters.Centroid.DATA_TYPE_ID,
					CentroidParameters.Centroid.DATA_NAMESPACE_URI,
					CentroidParameters.Centroid.EXTRACTOR_CLASS,
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS
				});
		ClusteringParameters.fillOptions(
				options,
				new ClusteringParameters.Clustering[] {
					ClusteringParameters.Clustering.MAX_REDUCER_COUNT,
					ClusteringParameters.Clustering.MAX_ITERATIONS,
					ClusteringParameters.Clustering.CONVERGANCE_TOLERANCE
				});
		CommonParameters.fillOptions(
				options,
				new CommonParameters.Common[] {
					CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
					CommonParameters.Common.HDFS_INPUT_PATH
				});

		CentroidManagerGeoWave.fillOptions(options);
		NestedGroupCentroidAssignment.fillOptions(options);
	}

	@Override
	public int run(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return this.run(
				MapReduceJobController.getConfiguration(runTimeProperties),
				runTimeProperties);
	}
}