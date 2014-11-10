package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import mil.nga.giat.geowave.ingest.AbstractCommandLineDriver;
import mil.nga.giat.geowave.ingest.AccumuloCommandLineOptions;
import mil.nga.giat.geowave.ingest.IngestTypePluginProviderSpi;
import mil.nga.giat.geowave.ingest.hdfs.HdfsCommandLineOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * This class actually executes the ingestion of intermediate data into GeoWave
 * that had been staged in HDFS.
 */
public class IngestFromHdfsDriver extends
		AbstractCommandLineDriver
{
	private final static Logger LOGGER = Logger.getLogger(IngestFromHdfsDriver.class);
	private final static int NUM_CONCURRENT_JOBS = 5;
	private final static int DAYS_TO_AWAIT_COMPLETION = 999;
	private HdfsCommandLineOptions hdfsOptions;
	private AccumuloCommandLineOptions accumuloOptions;
	private MapReduceCommandLineOptions mapReduceOptions;
	private static ExecutorService singletonExecutor;

	public IngestFromHdfsDriver(
			final String operation ) {
		super(
				operation);
	}

	private static synchronized ExecutorService getSingletonExecutorService() {
		if (singletonExecutor == null || singletonExecutor.isShutdown()) {
			singletonExecutor = Executors.newFixedThreadPool(NUM_CONCURRENT_JOBS);
		}
		return singletonExecutor;
	}

	@Override
	protected void runInternal(
			final String[] args,
			final List<IngestTypePluginProviderSpi<?, ?>> pluginProviders ) {
		final Configuration conf = new Configuration();
		conf.set(
				"fs.defaultFS",
				 hdfsOptions.getHdfsHostPort());
		conf.set(
				"fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set(
				"mapred.job.tracker",
				mapReduceOptions.getJobTrackerHostPort());
		final Path hdfsBaseDirectory = new Path(
				hdfsOptions.getBasePath());
		try {
			final FileSystem fs = FileSystem.get(conf);
			if (!fs.exists(hdfsBaseDirectory)) {
				LOGGER.fatal("HDFS base directory " + hdfsBaseDirectory + " does not exist");
				return;
			}
			for (final IngestTypePluginProviderSpi<?, ?> pluginProvider : pluginProviders) {
				// if an appropriate sequence file does not exist, continue

				// TODO: we should probably clean up the type name to make it
				// HDFS path safe in case there are invalid characters
				final Path inputFile = new Path(
						hdfsBaseDirectory,
						pluginProvider.getIngestTypeName());
				if (!fs.exists(inputFile)) {
					LOGGER.warn("HDFS file '" + inputFile + "' does not exist for ingest type '" + pluginProvider.getIngestTypeName() + "'");
					continue;
				}
				IngestFromHdfsPlugin ingestFromHdfsPlugin = null;
				try {
					ingestFromHdfsPlugin = pluginProvider.getIngestFromHdfsPlugin();

					if (ingestFromHdfsPlugin == null) {
						LOGGER.warn("Plugin provider for ingest type '" + pluginProvider.getIngestTypeName() + "' does not support ingest from HDFS");
						continue;
					}
					if (!accumuloOptions.isSupported(ingestFromHdfsPlugin.getSupportedIndices())) {
						LOGGER.warn("HDFS file ingest plugin for ingest type '" + pluginProvider.getIngestTypeName() + "' does not support dimensionality '" + accumuloOptions.getType().name() + "'");
						continue;
					}
				}
				catch (final UnsupportedOperationException e) {
					LOGGER.warn(
							"Plugin provider '" + pluginProvider.getIngestTypeName() + "' does not support ingest from HDFS",
							e);
					continue;
				}

				IngestWithReducer ingestWithReducer = null;
				IngestWithMapper ingestWithMapper = null;

				// first find one preferred method of ingest from HDFS
				// (exclusively setting one or the other instance above)
				if (ingestFromHdfsPlugin.isUseReducerPreferred()) {
					ingestWithReducer = ingestFromHdfsPlugin.ingestWithReducer();
					if (ingestWithReducer == null) {
						LOGGER.warn("Plugin provider '" + pluginProvider.getIngestTypeName() + "' prefers ingest with reducer but it is unimplemented");
					}
				}
				if (ingestWithReducer == null) {
					// check for ingest with mapper
					ingestWithMapper = ingestFromHdfsPlugin.ingestWithMapper();
					if ((ingestWithMapper == null) && !ingestFromHdfsPlugin.isUseReducerPreferred()) {

						ingestWithReducer = ingestFromHdfsPlugin.ingestWithReducer();
						if (ingestWithReducer == null) {
							LOGGER.warn("Plugin provider '" + pluginProvider.getIngestTypeName() + "' does not does not support ingest from HDFS");
							continue;
						}
						else {
							LOGGER.warn("Plugin provider '" + pluginProvider.getIngestTypeName() + "' prefers ingest with mapper but it is unimplemented");
						}
					}
				}

				AbstractMapReduceIngest jobRunner = null;
				if (ingestWithReducer != null) {
					jobRunner = new IngestWithReducerJobRunner(
							accumuloOptions,
							inputFile,
							pluginProvider.getIngestTypeName(),
							ingestFromHdfsPlugin,
							ingestWithReducer);

				}
				else if (ingestWithMapper != null) {
					jobRunner = new IngestWithMapperJobRunner(
							accumuloOptions,
							inputFile,
							pluginProvider.getIngestTypeName(),
							ingestFromHdfsPlugin,
							ingestWithMapper);

				}
				if (jobRunner != null) {
					try {
						runJob(
								conf,
								jobRunner,
								args);
					}
					catch (final Exception e) {
						LOGGER.warn(
								"Error running ingest job",
								e);
					}
				}
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Error in accessing HDFS file system",
					e);
		}
		finally {
			final ExecutorService executorService = getSingletonExecutorService();
			executorService.shutdown();
			// do we want to just exit once our jobs are submitted or wait?
			// for now let's just wait a REALLY long time until all of the
			// submitted jobs complete
			try {
				executorService.awaitTermination(
						DAYS_TO_AWAIT_COMPLETION,
						TimeUnit.DAYS);
			}
			catch (final InterruptedException e) {
				LOGGER.error(
						"Error waiting for submitted jobs to complete",
						e);
			}
		}
	}

	private void runJob(
			final Configuration conf,
			final AbstractMapReduceIngest jobRunner,
			final String[] args )
			throws Exception {
		final ExecutorService executorService = getSingletonExecutorService();
		executorService.execute(new Runnable() {

			@Override
			public void run() {
				try {
					final int res = ToolRunner.run(
							conf,
							jobRunner,
							args);
					if (res != 0) {
						LOGGER.error("Mapper ingest job '" + jobRunner.getJobName() + "' exited with error code: " + res);
					}
				}
				catch (final Exception e) {
					LOGGER.error(
							"Error running mapper ingest job: " + jobRunner.getJobName(),
							e);
				}
			}
		});
	}

	@Override
	public void parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		accumuloOptions = AccumuloCommandLineOptions.parseOptions(commandLine);
		hdfsOptions = HdfsCommandLineOptions.parseOptions(commandLine);
		mapReduceOptions = MapReduceCommandLineOptions.parseOptions(commandLine);
	}

	@Override
	public void applyOptions(
			final Options allOptions ) {
		AccumuloCommandLineOptions.applyOptions(allOptions);
		HdfsCommandLineOptions.applyOptions(allOptions);
		MapReduceCommandLineOptions.applyOptions(allOptions);
	}
}
