package mil.nga.giat.geowave.analytic.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.analytic.PropertyManagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ToolRunnerMapReduceIntegration implements
		MapReduceIntegration
{

	@Override
	public Job getJob(
			final Tool tool )
			throws IOException {
		return new Job(
				tool.getConf());
	}

	@Override
	public int submit(
			final Configuration configuration,
			final PropertyManagement runTimeProperties,
			final GeoWaveAnalyticJobRunner tool )
			throws Exception {
		return ToolRunner.run(
				configuration,
				tool,
				new String[] {});
	}

	@Override
	public Counters waitForCompletion(
			final Job job )
			throws ClassNotFoundException,
			InterruptedException,
			Exception {
		final boolean status = job.waitForCompletion(true);
		return status ? job.getCounters() : null;

	}

	@Override
	public Configuration getConfiguration(
			final PropertyManagement runTimeProperties )
			throws IOException {
		return MapReduceJobController.getConfiguration(runTimeProperties);
	}

}
