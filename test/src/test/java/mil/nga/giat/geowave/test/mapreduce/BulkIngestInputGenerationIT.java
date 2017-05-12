package mil.nga.giat.geowave.test.mapreduce;

import java.io.IOException;

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import mil.nga.giat.geowave.examples.ingest.bulk.GeonamesDataFileInputFormat;
import mil.nga.giat.geowave.examples.ingest.bulk.SimpleFeatureToAccumuloKeyValueMapper;

public class BulkIngestInputGenerationIT
{

	private static final Logger LOGGER = LoggerFactory.getLogger(BulkIngestInputGenerationIT.class);
	private static final String TEST_DATA_LOCATION = "src/test/resources/mil/nga/giat/geowave/test/geonames/barbados";
	private static final long NUM_GEONAMES_RECORDS = 834; // (see BB.txt)
	private static final String OUTPUT_PATH = "target/tmp_bulkIngestTest";
	private static long mapInputRecords;
	private static long mapOutputRecords;

	private static long startMillis;

	@BeforeClass
	public static void startTimer() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*  RUNNING BulkIngestInputGenerationIT  *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED BulkIngestInputGenerationIT  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testMapReduceJobSuccess()
			throws Exception {

		LOGGER.info("Running Bulk Ingest Input Generation MapReduce job...");

		final int exitCode = ToolRunner.run(
				new BulkIngestInputGenerationJobRunner(),
				null);

		LOGGER.info("Job completed with exit code: " + exitCode);

		// verify exitCode = 0
		Assert.assertEquals(
				exitCode,
				0);

		verifyNumInputRecords();

		verifyNumAccumuloKeyValuePairs();

		verifyJobOutput();
	}

	private void verifyNumInputRecords() {
		Assert.assertEquals(
				mapInputRecords,
				NUM_GEONAMES_RECORDS);
	}

	private void verifyNumAccumuloKeyValuePairs() {
		Assert.assertEquals(
				mapOutputRecords,
				(NUM_GEONAMES_RECORDS));
	}

	private void verifyJobOutput()
			throws IOException {

		final String _SUCCESS = "_SUCCESS";
		final String REDUCER_OUTPUT = "part-r-";
		boolean wasSuccessful = false;
		boolean reducerOutputExists = false;
		final FileSystem fs = FileSystem.getLocal(new Configuration());
		final RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(
				new Path(
						OUTPUT_PATH),
				false);
		LocatedFileStatus fileStatus = null;
		String fileName = null;

		while (iterator.hasNext()) {
			fileStatus = iterator.next();
			fileName = fileStatus.getPath().getName();

			if (fileName.contains(_SUCCESS)) {
				wasSuccessful = true;
			}
			if (fileName.contains(REDUCER_OUTPUT)) {
				reducerOutputExists = true;
			}
		}

		// verify presence of _SUCCESS file
		Assert.assertEquals(
				wasSuccessful,
				true);

		// verify presence of Reducer output
		Assert.assertEquals(
				reducerOutputExists,
				true);
	}

	private static class BulkIngestInputGenerationJobRunner extends
			Configured implements
			Tool
	{
		private static final String JOB_NAME = "BulkIngestInputGenerationITJob";
		private static final String TASK_COUNTER_GROUP_NAME = "org.apache.hadoop.mapreduce.TaskCounter";
		private static final String MAP_INPUT_RECORDS = "MAP_INPUT_RECORDS";
		private static final String MAP_OUTPUT_RECORDS = "MAP_OUTPUT_RECORDS";

		@Override
		public int run(
				final String[] args )
				throws Exception {

			final Configuration conf = getConf();
			conf.set(
					"fs.defaultFS",
					"file:///");

			final Job job = Job.getInstance(
					conf,
					JOB_NAME);
			job.setJarByClass(getClass());

			FileInputFormat.setInputPaths(
					job,
					new Path(
							TEST_DATA_LOCATION));
			FileOutputFormat.setOutputPath(
					job,
					cleanPathForReuse(
							conf,
							OUTPUT_PATH));

			job.setMapperClass(SimpleFeatureToAccumuloKeyValueMapper.class);
			job.setReducerClass(Reducer.class); // (Identity Reducer)

			job.setInputFormatClass(GeonamesDataFileInputFormat.class);
			job.setOutputFormatClass(AccumuloFileOutputFormat.class);

			job.setMapOutputKeyClass(Key.class);
			job.setMapOutputValueClass(Value.class);
			job.setOutputKeyClass(Key.class);
			job.setOutputValueClass(Value.class);

			job.setNumReduceTasks(1);
			job.setSpeculativeExecution(false);

			final boolean result = job.waitForCompletion(true);

			mapInputRecords = job.getCounters().findCounter(
					TASK_COUNTER_GROUP_NAME,
					MAP_INPUT_RECORDS).getValue();

			mapOutputRecords = job.getCounters().findCounter(
					TASK_COUNTER_GROUP_NAME,
					MAP_OUTPUT_RECORDS).getValue();

			return result ? 0 : 1;
		}

		private Path cleanPathForReuse(
				final Configuration conf,
				final String pathString )
				throws IOException {

			final FileSystem fs = FileSystem.get(conf);
			final Path path = new Path(
					pathString);

			if (fs.exists(path)) {
				LOGGER.info("Deleting '" + pathString + "' for reuse.");
				fs.delete(
						path,
						true);
			}

			return path;
		}
	}

}
