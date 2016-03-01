package mil.nga.giat.geowave.core.ingest.local;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.cli.CommandLineResult;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.IngestUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * This extends the local file driver to directly ingest data into GeoWave
 * utilizing the LocalFileIngestPlugin's that are discovered by the system.
 */
public class LocalFileIngestDriver extends
		AbstractLocalFileDriver<LocalFileIngestPlugin<?>, LocalIngestRunData>
{
	public final static int INGEST_BATCH_SIZE = 500;
	private final static Logger LOGGER = Logger.getLogger(LocalFileIngestDriver.class);
	protected DataStoreCommandLineOptions dataStoreOptions;
	protected IngestCommandLineOptions ingestOptions;
	protected ExecutorService ingestExecutor;

	public LocalFileIngestDriver(
			final String operation ) {
		super(
				operation);
	}

	@Override
	protected void parseOptionsInternal(
			final Options options,
			CommandLine commandLine )
			throws ParseException {
		final CommandLineResult<DataStoreCommandLineOptions> dataStoreOptionsResult = DataStoreCommandLineOptions.parseOptions(
				options,
				commandLine);
		dataStoreOptions = dataStoreOptionsResult.getResult();
		if (dataStoreOptionsResult.isCommandLineChange()) {
			commandLine = dataStoreOptionsResult.getCommandLine();
		}
		ingestOptions = IngestCommandLineOptions.parseOptions(commandLine);
		super.parseOptionsInternal(
				options,
				commandLine);
	}

	@Override
	protected void applyOptionsInternal(
			final Options allOptions ) {
		DataStoreCommandLineOptions.applyOptions(allOptions);
		IngestCommandLineOptions.applyOptions(allOptions);
		super.applyOptionsInternal(allOptions);
	}

	@Override
	protected boolean runInternal(
			final String[] args,
			final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders ) {
		// first collect the local file ingest plugins
		final Map<String, LocalFileIngestPlugin<?>> localFileIngestPlugins = new HashMap<String, LocalFileIngestPlugin<?>>();
		final List<WritableDataAdapter<?>> adapters = new ArrayList<WritableDataAdapter<?>>();
		for (final IngestFormatPluginProviderSpi<?, ?> pluginProvider : pluginProviders) {
			LocalFileIngestPlugin<?> localFileIngestPlugin = null;
			try {
				localFileIngestPlugin = pluginProvider.getLocalFileIngestPlugin();

				if (localFileIngestPlugin == null) {
					LOGGER.warn("Plugin provider for ingest type '" + pluginProvider.getIngestFormatName() + "' does not support local file ingest");
					continue;
				}
			}
			catch (final UnsupportedOperationException e) {
				LOGGER.warn(
						"Plugin provider '" + pluginProvider.getIngestFormatName() + "' does not support local file ingest",
						e);
				continue;
			}
			final boolean indexSupported = (IngestUtils.isSupported(
					localFileIngestPlugin,
					args,
					ingestOptions.getDimensionalityTypes()));
			if (!indexSupported) {
				LOGGER.warn("Local file ingest plugin for ingest type '" + pluginProvider.getIngestFormatName() + "' does not support dimensionality type '" + ingestOptions.getDimensionalityTypeArgument() + "'");
				continue;
			}
			localFileIngestPlugins.put(
					pluginProvider.getIngestFormatName(),
					localFileIngestPlugin);
			adapters.addAll(Arrays.asList(localFileIngestPlugin.getDataAdapters(ingestOptions.getVisibility())));
		}

		final DataStore dataStore = dataStoreOptions.createStore();
		try (LocalIngestRunData runData = new LocalIngestRunData(
				adapters,
				dataStore,
				args)) {

			startExecutor();

			processInput(
					localFileIngestPlugins,
					runData);

			// We place this here and not just in finally because of the way
			// that try-with-resources works.
			// We want to wait for our ingesting threads to finish before we
			// kill our index writers, which
			// are cached in LocalIngestRunData. If we were don't, then the
			// index writers will be
			// closed before they are finished processing the file entries.
			shutdownExecutor();
		}
		catch (final IOException e) {
			LOGGER.fatal(
					"Unexpected I/O exception when reading input files",
					e);
			return false;
		}
		finally {
			shutdownExecutor();
		}
		return true;
	}

	/**
	 * Create a basic thread pool to ingest file data. We limit it to the amount
	 * of threads specified on the command line.
	 */
	private void startExecutor() {
		ingestExecutor = Executors.newFixedThreadPool(localInput.getThreads());
	}

	/**
	 * This function will wait for executing tasks to complete for up to 10
	 * seconds.
	 */
	private void shutdownExecutor() {
		if (ingestExecutor != null) {
			try {
				ingestExecutor.shutdown();
				while (!ingestExecutor.awaitTermination(
						10,
						TimeUnit.SECONDS)) {
					LOGGER.debug("Waiting for ingest executor to terminate");
				}
			}
			catch (InterruptedException e) {
				LOGGER.error("Failed to terminate executor service");
			}
			finally {
				ingestExecutor = null;
			}
		}
	}

	@Override
	protected void processFile(
			final File file,
			final String typeName,
			final LocalFileIngestPlugin<?> plugin,
			final LocalIngestRunData ingestRunData )
			throws IOException {

		LOGGER.info(String.format(
				"Beginning ingest for file: [%s]",
				file.getName()));

		// This loads up the primary indexes that are specified on the command
		// line.
		// Usually spatial or spatial-temporal
		final String[] dimensionTypes = ingestOptions.getDimensionalityTypes();
		final Map<ByteArrayId, PrimaryIndex> specifiedPrimaryIndexes = new HashMap<ByteArrayId, PrimaryIndex>();
		for (final String dimensionType : dimensionTypes) {
			final PrimaryIndex primaryIndex = IngestUtils.getIndex(
					plugin,
					ingestRunData.getArgs(),
					dimensionType);
			if (primaryIndex == null) {
				LOGGER.error("Could not get index instance, getIndex() returned null;");
				throw new IOException(
						"Could not get index instance, getIndex() returned null");
			}
			specifiedPrimaryIndexes.put(
					primaryIndex.getId(),
					primaryIndex);
		}

		// This gets the list of required indexes from the Plugin.
		// If for some reason a GeoWaveData specifies an index that isn't
		// originally
		// in the specifiedPrimaryIndexes list, then this array is used to
		// determine
		// if the Plugin supports it. If it does, then we allow the creation of
		// the
		// index.
		final Map<ByteArrayId, PrimaryIndex> requiredIndexMap = new HashMap<ByteArrayId, PrimaryIndex>();
		final PrimaryIndex[] requiredIndices = plugin.getRequiredIndices();
		if ((requiredIndices != null) && (requiredIndices.length > 0)) {
			for (final PrimaryIndex requiredIndex : requiredIndices) {
				requiredIndexMap.put(
						requiredIndex.getId(),
						requiredIndex);
			}
		}

		// Create our queue. We will post GeoWaveData items to these queue until
		// there are no more items, at which point we will tell the workers to
		// complete. Ingest batch size is the total max number of items to read
		// from the file at a time for the worker threads to execute.
		BlockingQueue<GeoWaveData<?>> queue = LocalIngestRunData.createBlockingQueue(INGEST_BATCH_SIZE);

		// Create our Jobs. We submit as many jobs as we have executors for.
		// These folks will read our blocking queue
		LOGGER.debug(String.format(
				"Creating [%d] threads to ingest file: [%s]",
				localInput.getThreads(),
				file.getName()));
		List<IngestTask> ingestTasks = new ArrayList<IngestTask>();
		try {
			for (int i = 0; i < localInput.getThreads(); i++) {
				IngestTask task = new IngestTask(
						ingestRunData,
						specifiedPrimaryIndexes,
						requiredIndexMap,
						queue);
				ingestTasks.add(task);
				ingestExecutor.submit(task);
			}

			// Read files until EOF from the command line.
			try (CloseableIterator<?> geowaveDataIt = plugin.toGeoWaveData(
					file,
					specifiedPrimaryIndexes.keySet(),
					ingestOptions.getVisibility())) {

				while (geowaveDataIt.hasNext()) {
					final GeoWaveData<?> geowaveData = (GeoWaveData<?>) geowaveDataIt.next();
					try {
						queue.put(geowaveData);
					}
					catch (InterruptedException e) {
						// I can't see how this will ever happen, except maybe
						// someone kills the process?
						throw new RuntimeException(
								"Interrupted placing GeoWaveData on queue");
					}
				}
			}
		}
		finally {
			// Terminate our ingest tasks.
			for (IngestTask task : ingestTasks) {
				task.terminate();
			}
		}

		LOGGER.info(String.format(
				"Finished ingest for file: [%s]",
				file.getName()));
	}
}
