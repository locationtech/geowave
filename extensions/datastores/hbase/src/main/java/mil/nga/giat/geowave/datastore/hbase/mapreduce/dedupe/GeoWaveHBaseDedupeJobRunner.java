/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce.dedupe;

import mil.nga.giat.geowave.datastore.hbase.mapreduce.GeoWaveHBaseJobRunner;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.input.GeoWaveHBaseInputFormat;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.input.GeoWaveHBaseInputKey;

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
 * @author viggy Functionality similar to <code> GeoWaveDedupeJobRunner </code>
 */
public class GeoWaveHBaseDedupeJobRunner extends
		GeoWaveHBaseJobRunner
{

	@Override
	protected void configure(
			Job job )
			throws Exception {
		job.setJobName("GeoWave Dedupe (" + namespace + ")");

		job.setMapperClass(GeoWaveHBaseDedupeMapper.class);
		job.setCombinerClass(GeoWaveHBaseDedupeCombiner.class);
		job.setReducerClass(getReducer());
		job.setMapOutputKeyClass(GeoWaveHBaseInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setOutputKeyClass(GeoWaveHBaseInputKey.class);
		job.setOutputValueClass(ObjectWritable.class);

		job.setInputFormatClass(GeoWaveHBaseInputFormat.class);
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
		return GeoWaveHBaseDedupeReducer.class;
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
				new GeoWaveHBaseDedupeJobRunner(),
				args);
		System.exit(res);
	}
}
