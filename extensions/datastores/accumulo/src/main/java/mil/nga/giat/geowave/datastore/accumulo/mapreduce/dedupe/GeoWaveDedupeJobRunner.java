package mil.nga.giat.geowave.datastore.accumulo.mapreduce.dedupe;

import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveJobRunner;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class can run a basic job to query GeoWave, deduplicating results, and
 * writing the final set of key value pairs to a sequence file. It can be
 * extended for more advanced capabilities or job chaining.
 */
public class GeoWaveDedupeJobRunner extends
		GeoWaveJobRunner
{

	@Override
	protected void configure(
			final Job job )
			throws Exception {

		job.setJobName("GeoWave Dedupe (" + namespace + ")");

		job.setMapperClass(GeoWaveDedupeMapper.class);
		job.setCombinerClass(GeoWaveDedupeCombiner.class);
		job.setReducerClass(getReducer());
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setOutputKeyClass(GeoWaveInputKey.class);
		job.setOutputValueClass(ObjectWritable.class);

		job.setInputFormatClass(GeoWaveInputFormat.class);
		job.setOutputFormatClass(getOutputFormatClass());
		job.setNumReduceTasks(getNumReduceTasks());

		job.setSpeculativeExecution(false);

		final FileSystem fs = FileSystem.get(job.getConfiguration());
		final Path outputPath = getHdfsOutputPath();
		fs.delete(
				outputPath,
				true);
		FileOutputFormat.setOutputPath(
				job,
				outputPath);

	}

	protected String getHdfsOutputBase() {
		return "/tmp";
	}

	@SuppressWarnings("rawtypes")
	protected Class<? extends Reducer> getReducer() {
		return GeoWaveDedupeReducer.class;
	}

	public Path getHdfsOutputPath() {
		return new Path(
				getHdfsOutputBase() + "/" + namespace + "_dedupe");
	}

	protected Class<? extends OutputFormat> getOutputFormatClass() {
		return SequenceFileOutputFormat.class;
	}

	protected int getNumReduceTasks() {
		return 8;
	}

	public static void main(
			final String[] args )
			throws Exception {
		final int res = ToolRunner.run(
				new Configuration(),
				new GeoWaveDedupeJobRunner(),
				args);
		System.exit(res);
	}

}
