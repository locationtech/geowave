package mil.nga.giat.geowave.ingest.local;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.ingest.AccumuloCommandLineOptions;
import mil.nga.giat.geowave.ingest.GeoWaveData;
import mil.nga.giat.geowave.ingest.IngestRunData;
import mil.nga.giat.geowave.ingest.IngestTypePluginProviderSpi;
import mil.nga.giat.geowave.ingest.MainCommandLineOptions.Operation;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.IndexWriter;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class LocalFileIngestDriver extends
		AbstractLocalFileDriver<LocalFileIngestPlugin<?>, IngestRunData>
{
	private final static Logger LOGGER = Logger.getLogger(LocalFileIngestDriver.class);
	protected AccumuloCommandLineOptions accumulo;
	protected IndexWriter indexWriter;

	public LocalFileIngestDriver(
			final Operation operation ) {
		super(
				operation);
	}

	@Override
	public void parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		accumulo = AccumuloCommandLineOptions.parseOptions(commandLine);
		super.parseOptions(commandLine);
	}

	@Override
	public void applyOptions(
			final Options allOptions ) {
		AccumuloCommandLineOptions.applyOptions(allOptions);
		super.applyOptions(allOptions);
	}

	@Override
	protected void runInternal(
			final String[] args,
			final List<IngestTypePluginProviderSpi<?, ?>> pluginProviders ) {

		// first collect the local file ingest plugins
		final Map<String, LocalFileIngestPlugin<?>> localFileIngestPlugins = new HashMap<String, LocalFileIngestPlugin<?>>();
		final List<WritableDataAdapter<?>> adapters = new ArrayList<WritableDataAdapter<?>>();
		for (final IngestTypePluginProviderSpi<?, ?> pluginProvider : pluginProviders) {
			LocalFileIngestPlugin<?> localFileIngestPlugin = null;
			try {
				localFileIngestPlugin = pluginProvider.getLocalFileIngestPlugin();

				if (localFileIngestPlugin == null) {
					LOGGER.warn("Plugin provider for ingest type '" + pluginProvider.getIngestTypeName() + "' does not support local file ingest");
					continue;
				}
			}
			catch (final UnsupportedOperationException e) {
				LOGGER.warn(
						"Plugin provider '" + pluginProvider.getIngestTypeName() + "' does not support local file ingest",
						e);
				continue;
			}
			localFileIngestPlugins.put(
					pluginProvider.getIngestTypeName(),
					localFileIngestPlugin);
			adapters.addAll(Arrays.asList(localFileIngestPlugin.getDataAdapters(accumulo.getVisibility())));
		}

		try {
			final AccumuloOperations operations = accumulo.getAccumuloOperations();
			if (accumulo.isClearNamespace()) {
				operations.deleteAll();
			}
			if (localFileIngestPlugins.isEmpty()) {
				LOGGER.fatal("There were no local file ingest type plugin providers found");
				return;
			}
			final DataStore dataStore = new AccumuloDataStore(
					operations);
			indexWriter = null;
			try {
				indexWriter = dataStore.createIndexWriter(accumulo.getIndex());
				processInput(
						localFileIngestPlugins,
						new IngestRunData(
								indexWriter,
								adapters));
			}
			catch (final IOException e) {
				LOGGER.fatal(
						"Unexpected I/O exception when reading input files",
						e);
			}
			finally {
				if (indexWriter != null) {
					indexWriter.close();
				}
			}
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.fatal(
					"Unable to connect to Accumulo to ingest data",
					e);
		}
	}

	@Override
	protected void processFile(
			final File file,
			final String typeName,
			final LocalFileIngestPlugin plugin,
			final IngestRunData ingestRunData ) {
		final Iterable<GeoWaveData<?>> geowaveDataIt = plugin.toGeoWaveData(
				file,
				accumulo.getVisibility());
		for (final GeoWaveData<?> geowaveData : geowaveDataIt) {
			final WritableDataAdapter adapter = ingestRunData.getDataAdapter(geowaveData);
			if (adapter == null) {
				LOGGER.warn("Adapter not found for " + geowaveData.getData());
				continue;
			}
			indexWriter.write(
					adapter,
					geowaveData.getData());
		}

	}
}
