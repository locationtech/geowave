package mil.nga.giat.geowave.analytics.clustering.runners;

import mil.nga.giat.geowave.analytics.kmeans.mapreduce.runners.KMeansJumpJobRunner;

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
