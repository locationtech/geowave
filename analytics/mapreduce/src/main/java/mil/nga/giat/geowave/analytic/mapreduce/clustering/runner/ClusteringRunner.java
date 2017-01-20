package mil.nga.giat.geowave.analytic.mapreduce.clustering.runner;

import mil.nga.giat.geowave.analytic.IndependentJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;

public interface ClusteringRunner extends
		MapReduceJobRunner,
		IndependentJobRunner
{
	public void setInputFormatConfiguration(
			FormatConfiguration formatConfiguration );

	public void setZoomLevel(
			int zoomLevel );

}
