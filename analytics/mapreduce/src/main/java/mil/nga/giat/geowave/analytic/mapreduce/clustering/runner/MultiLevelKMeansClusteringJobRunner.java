package mil.nga.giat.geowave.analytic.mapreduce.clustering.runner;

import mil.nga.giat.geowave.analytic.mapreduce.kmeans.runner.KMeansParallelJobRunner;

/**
 * 
 * @see KMeansParallelJobRunner
 * 
 */
public class MultiLevelKMeansClusteringJobRunner extends
		MultiLevelClusteringJobRunner
{

	@Override
	protected ClusteringRunner getClusteringRunner() {
		return new KMeansParallelJobRunner();
	}
}
