package mil.nga.giat.geowave.analytics.clustering.runners;

import mil.nga.giat.geowave.analytics.tools.IndependentJobRunner;
import mil.nga.giat.geowave.analytics.tools.mapreduce.FormatConfiguration;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;

public interface ClusteringRunner extends
		MapReduceJobRunner,
		IndependentJobRunner
{
	public void setInputFormatConfiguration(
			FormatConfiguration formatConfiguration );

	public void setZoomLevel(
			int zoomLevel );

}
