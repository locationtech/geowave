package mil.nga.giat.geowave.core.ingest.local;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.cli.CommandLineResult;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.IngestCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.IngestUtils;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * This extends the local file driver to directly ingest data into GeoWave
 * utilizing the LocalFileIngestPlugin's that are discovered by the system.
 */
public class LocalFileIngestDriver extends
		AbstractLocalFileDriver<LocalFileIngestPlugin<?>, IngestRunData>
{
	private final static Logger LOGGER = Logger.getLogger(LocalFileIngestDriver.class);
	protected DataStoreCommandLineOptions dataStoreOptions;
	protected IngestCommandLineOptions ingestOptions;

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
			final boolean indexSupported = (ingestOptions.getIndex(
					localFileIngestPlugin,
					args) != null);
			if (!indexSupported) {
				LOGGER.warn("Local file ingest plugin for ingest type '" + pluginProvider.getIngestFormatName() + "' does not support dimensionality type '" + ingestOptions.getDimensionalityType() + "'");
				continue;
			}
			localFileIngestPlugins.put(
					pluginProvider.getIngestFormatName(),
					localFileIngestPlugin);
			adapters.addAll(Arrays.asList(localFileIngestPlugin.getDataAdapters(ingestOptions.getVisibility())));
		}

		final DataStore dataStore = dataStoreOptions.createStore();
		try (IngestRunData runData = new IngestRunData(
				adapters,
				dataStore,
				args)) {
			processInput(
					localFileIngestPlugins,
					runData);
		}
		catch (final IOException e) {
			LOGGER.fatal(
					"Unexpected I/O exception when reading input files",
					e);
			return false;
		}
		return true;
	}

	@Override
	protected void processFile(
			final File file,
			final String typeName,
			final LocalFileIngestPlugin<?> plugin,
			final IngestRunData ingestRunData )
			throws IOException {
		IngestUtils.ingest(
				file,
				ingestOptions,
				plugin,
				plugin,
				ingestRunData);
	}
}
