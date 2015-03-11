package mil.nga.giat.geowave.analytics.tools.mapreduce;

import mil.nga.giat.geowave.analytics.tools.PropertyManagement;

import org.apache.hadoop.conf.Configuration;

public interface MapReduceJobRunner
{
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception;
}
