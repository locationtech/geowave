package mil.nga.giat.geowave.core.store.cli.remote;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.StatsCommandLineOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.StoreLoader;

/**
 * Common methods for dumping and calculating stats.
 */
public abstract class AbstractStatsCommand extends
		DefaultOperation
{
	private static final Logger LOGGER = LoggerFactory.getLogger(RecalculateStatsCommand.class);

	@ParametersDelegate
	private StatsCommandLineOptions statsOptions = new StatsCommandLineOptions();

	private DataStorePluginOptions inputStoreOptions = null;

	public void run(
			OperationParams params,
			List<String> parameters ) {

		String storeName = parameters.get(0);
		String adapterIdName = null;
		if (parameters.size() > 1) {
			adapterIdName = parameters.get(1);
		}

		// Attempt to load store.
		File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

		// Attempt to load input store.
		if (inputStoreOptions == null) {
			StoreLoader inputStoreLoader = new StoreLoader(
					storeName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}

		try {

			// Various stores needed
			AdapterStore adapterStore = inputStoreOptions.createAdapterStore();

			final String[] authorizations = getAuthorizations(statsOptions.getAuthorizations());

			if (adapterIdName != null) {
				final ByteArrayId adapterId = new ByteArrayId(
						adapterIdName);
				DataAdapter<?> adapter = adapterStore.getAdapter(adapterId);
				if (adapter == null) {
					LOGGER.error("Unknown adapter " + adapterId);
					final CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters();
					final StringBuffer buffer = new StringBuffer();
					while (it.hasNext()) {
						adapter = it.next();
						buffer.append(
								adapter.getAdapterId().getString()).append(
								' ');
					}
					it.close();
					LOGGER.info("Available adapters: " + buffer.toString());
				}
				calculateStatistics(
						inputStoreOptions,
						adapter,
						authorizations);
			}
			else {
				try (CloseableIterator<DataAdapter<?>> adapterIt = adapterStore.getAdapters()) {
					while (adapterIt.hasNext()) {
						final DataAdapter<?> adapter = adapterIt.next();
						if (!calculateStatistics(
								inputStoreOptions,
								adapter,
								authorizations)) {
							LOGGER.info("Unable to calculate statistics for adapter: "
									+ adapter.getAdapterId().getString());
						}
					}
				}
			}
		}
		catch (final IOException e) {
			throw new RuntimeException(
					"Unable to parse stats tool arguments",
					e);
		}
	}

	abstract protected boolean calculateStatistics(
			final DataStorePluginOptions options,
			final DataAdapter<?> adapter,
			final String[] authorizations )
			throws IOException;

	private static String[] getAuthorizations(
			final String auths ) {
		if ((auths == null) || (auths.length() == 0)) {
			return new String[0];
		}
		final String[] authsArray = auths.split(",");
		for (int i = 0; i < authsArray.length; i++) {
			authsArray[i] = authsArray[i].trim();
		}
		return authsArray;
	}

	public StatsCommandLineOptions getStatsOptions() {
		return statsOptions;
	}

	public void setStatsOptions(
			StatsCommandLineOptions statsOptions ) {
		this.statsOptions = statsOptions;
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setInputStoreOptions(
			DataStorePluginOptions inputStoreOptions ) {
		this.inputStoreOptions = inputStoreOptions;
	}
}