package mil.nga.giat.geowave.analytic.mapreduce;

import mil.nga.giat.geowave.analytic.PropertyManagement;

import org.apache.hadoop.conf.Configuration;

public interface MapReduceJobRunner
{
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception;
}
