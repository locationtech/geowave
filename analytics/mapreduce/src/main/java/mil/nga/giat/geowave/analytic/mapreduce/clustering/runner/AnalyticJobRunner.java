package mil.nga.giat.geowave.analytic.mapreduce.clustering.runner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public abstract class AnalyticJobRunner extends
		Configured implements
		Tool
{
	@SuppressWarnings("deprecation")
	public int runJob()
			throws IOException,
			InterruptedException,
			ClassNotFoundException {
		final Configuration conf = super.getConf();

		final Job job = Job.getInstance(conf);

		job.setJarByClass(this.getClass());

		final boolean jobSuccess = job.waitForCompletion(true);

		return (jobSuccess) ? 0 : 1;

	}

	protected abstract void configure(
			Job job )
			throws Exception;

	@Override
	public int run(
			final String[] args )
			throws Exception {
		return runJob();
	}
}
