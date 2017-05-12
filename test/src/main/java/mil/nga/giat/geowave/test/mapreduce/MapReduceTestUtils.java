package mil.nga.giat.geowave.test.mapreduce;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.ingest.operations.LocalToMapReduceToGeowaveCommand;
import mil.nga.giat.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;

public class MapReduceTestUtils
{

	private final static Logger LOGGER = LoggerFactory.getLogger(MapReduceTestUtils.class);

	public static final String EXPECTED_RESULTS_KEY = "EXPECTED_RESULTS";
	public static final int MIN_INPUT_SPLITS = 3;
	public static final int MAX_INPUT_SPLITS = 5;

	protected static void testMapReduceIngest(
			final DataStorePluginOptions dataStore,
			final DimensionalityType dimensionalityType,
			final String ingestFilePath ) {
		testMapReduceIngest(
				dataStore,
				dimensionalityType,
				"gpx",
				ingestFilePath);
	}

	protected static void testMapReduceIngest(
			final DataStorePluginOptions dataStore,
			final DimensionalityType dimensionalityType,
			final String format,
			final String ingestFilePath ) {
		// ingest gpx data directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments
		LOGGER.warn("Ingesting '" + ingestFilePath + "' - this may take several minutes...");

		final Thread progressLogger = startProgressLogger();

		// Indexes
		final String[] indexTypes = dimensionalityType.getDimensionalityArg().split(
				",");
		final List<IndexPluginOptions> indexOptions = new ArrayList<IndexPluginOptions>(
				indexTypes.length);
		for (final String indexType : indexTypes) {
			final IndexPluginOptions indexOption = new IndexPluginOptions();
			indexOption.selectPlugin(indexType);
			indexOptions.add(indexOption);
		}
		// Ingest Formats
		final IngestFormatPluginOptions ingestFormatOptions = new IngestFormatPluginOptions();
		ingestFormatOptions.selectPlugin(format);

		final LocalToMapReduceToGeowaveCommand mrGw = new LocalToMapReduceToGeowaveCommand();

		mrGw.setInputIndexOptions(indexOptions);
		mrGw.setInputStoreOptions(dataStore);

		mrGw.setPluginFormats(ingestFormatOptions);
		final MapReduceTestEnvironment env = MapReduceTestEnvironment.getInstance();
		mrGw.setParameters(
				ingestFilePath,
				env.getHdfs(),
				env.getHdfsBaseDirectory(),
				null,
				null);
		mrGw.getMapReduceOptions().setJobTrackerHostPort(
				env.getJobtracker());

		mrGw.execute(new ManualOperationParams());

		progressLogger.interrupt();
	}

	private static Thread startProgressLogger() {
		final Runnable r = new Runnable() {
			@Override
			public void run() {
				final long start = System.currentTimeMillis();
				try {
					while (true) {
						final long now = System.currentTimeMillis();
						LOGGER.warn("Ingest running, progress: " + ((now - start) / 1000) + "s.");
						Thread.sleep(60000);
					}
				}
				catch (final InterruptedException e) {
					// Do nothing; thread is designed to be interrupted when
					// ingest completes
				}
			}
		};

		final Thread t = new Thread(
				r);

		t.start();

		return t;
	}

	public static void filterConfiguration(
			final Configuration conf ) {
		// final parameters, can't be overriden
		conf.unset("mapreduce.job.end-notification.max.retry.interval");
		conf.unset("mapreduce.job.end-notification.max.attempts");

		// deprecated parameters (added in by default since we used the
		// Configuration() constructor (everything is set))
		conf.unset("session.id");
		conf.unset("mapred.jar");
		conf.unset("fs.default.name");
		conf.unset("mapred.map.tasks.speculative.execution");
		conf.unset("mapred.reduce.tasks");
		conf.unset("mapred.reduce.tasks.speculative.execution");
		conf.unset("mapred.mapoutput.value.class");
		conf.unset("mapred.used.genericoptionsparser");
		conf.unset("mapreduce.map.class");
		conf.unset("mapred.job.name");
		conf.unset("mapreduce.inputformat.class");
		conf.unset("mapred.input.dir");
		conf.unset("mapreduce.outputformat.class");
		conf.unset("mapred.map.tasks");
		conf.unset("mapred.mapoutput.key.class");
		conf.unset("mapred.working.dir");
	}

	public static Configuration getConfiguration() {
		final Configuration conf = new Configuration();
		final MapReduceTestEnvironment env = MapReduceTestEnvironment.getInstance();
		conf.set(
				"fs.defaultFS",
				env.getHdfs());
		conf.set(
				"fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set(
				"mapreduce.jobtracker.address",
				env.getJobtracker());

		filterConfiguration(conf);

		return conf;

	}

}
