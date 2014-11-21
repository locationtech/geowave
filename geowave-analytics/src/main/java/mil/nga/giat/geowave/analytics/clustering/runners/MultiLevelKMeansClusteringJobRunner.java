package mil.nga.giat.geowave.analytics.clustering.runners;

import mil.nga.giat.geowave.analytics.kmeans.mapreduce.runners.KMeansParallelJobRunner;

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
