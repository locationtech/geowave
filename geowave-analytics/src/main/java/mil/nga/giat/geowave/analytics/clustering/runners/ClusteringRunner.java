package mil.nga.giat.geowave.analytics.clustering.runners;

import mil.nga.giat.geowave.analytics.tools.IndependentJobRunner;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;

import org.apache.hadoop.fs.Path;

public interface ClusteringRunner extends
		MapReduceJobRunner,
		IndependentJobRunner
{
	public void setInputHDFSPath(
			Path inputPath );

	public void setZoomLevel(
			int zoomLevel );

}
