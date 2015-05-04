package mil.nga.giat.geowave.analytic.mapreduce.clustering.runner;

import mil.nga.giat.geowave.analytic.mapreduce.kmeans.runner.KMeansJumpJobRunner;

/**
 * 
 * @see KMeansJumpJobRunner
 * 
 */
public class MultiLevelJumpKMeansClusteringJobRunner extends
		MultiLevelClusteringJobRunner
{

	@Override
	protected ClusteringRunner getClusteringRunner() {
		return new KMeansJumpJobRunner();
	}
}
