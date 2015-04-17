package mil.nga.giat.geowave.analytics.tools.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.analytics.tools.PropertyManagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public interface MapReduceIntegration
{
	public int submit(
			final Configuration configuration,
			final PropertyManagement runTimeProperties,
			final GeoWaveAnalyticJobRunner tool )
			throws Exception;

	public boolean waitForCompletion(
			Job job )
			throws InterruptedException,
			Exception;

	public Job getJob(
			Tool tool )
			throws IOException;
}
