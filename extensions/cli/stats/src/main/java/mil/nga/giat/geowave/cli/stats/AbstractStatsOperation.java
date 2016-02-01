package mil.nga.giat.geowave.cli.stats;

import java.io.IOException;

import mil.nga.giat.geowave.core.cli.AdapterStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.cli.CommandLineResult;
import mil.nga.giat.geowave.core.cli.DataStatisticsStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.IndexStoreCommandLineOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

abstract public class AbstractStatsOperation implements
		CLIOperationDriver
{
	private static final Logger LOGGER = Logger.getLogger(AbstractStatsOperation.class);

	abstract protected boolean calculateStatistics(
			final DataStore dataStore,
			final IndexStore indexStore,
			final DataStatisticsStore statsStore,
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

	@Override
	public boolean runOperation(
			final String[] args )
			throws ParseException {
		final Options allOptions = new Options();
		DataStoreCommandLineOptions.applyOptions(allOptions);
		AdapterStoreCommandLineOptions.applyOptions(allOptions);
		IndexStoreCommandLineOptions.applyOptions(allOptions);
		DataStatisticsStoreCommandLineOptions.applyOptions(allOptions);
		StatsCommandLineOptions.applyOptions(allOptions);
		try {
			CommandLine commandLine = new BasicParser().parse(
					allOptions,
					args,
					true);
			CommandLineResult<DataStoreCommandLineOptions> dataStoreCli = null;
			CommandLineResult<AdapterStoreCommandLineOptions> adapterStoreCli = null;
			CommandLineResult<IndexStoreCommandLineOptions> indexStoreCli = null;
			CommandLineResult<DataStatisticsStoreCommandLineOptions> statsStoreCli = null;
			StatsCommandLineOptions statsOptions = null;
			ParseException parseException = null;
			boolean newCommandLine = false;
			do {
				newCommandLine = false;
				dataStoreCli = null;
				adapterStoreCli = null;
				indexStoreCli = null;
				statsStoreCli = null;
				parseException = null;
				try {
					dataStoreCli = DataStoreCommandLineOptions.parseOptions(
							allOptions,
							commandLine);
				}
				catch (final ParseException e) {
					parseException = e;
				}
				if ((dataStoreCli != null) && dataStoreCli.isCommandLineChange()) {
					commandLine = dataStoreCli.getCommandLine();
				}
				try {
					adapterStoreCli = AdapterStoreCommandLineOptions.parseOptions(
							allOptions,
							commandLine);
				}
				catch (final ParseException e) {
					parseException = e;
				}
				if ((adapterStoreCli != null) && adapterStoreCli.isCommandLineChange()) {
					commandLine = adapterStoreCli.getCommandLine();
					newCommandLine = true;
					continue;
				}
				try {
					indexStoreCli = IndexStoreCommandLineOptions.parseOptions(
							allOptions,
							commandLine);
				}
				catch (final ParseException e) {
					parseException = e;
				}
				if ((indexStoreCli != null) && indexStoreCli.isCommandLineChange()) {
					commandLine = indexStoreCli.getCommandLine();
					newCommandLine = true;
					continue;
				}
				try {
					statsStoreCli = DataStatisticsStoreCommandLineOptions.parseOptions(
							allOptions,
							commandLine);
				}
				catch (final ParseException e) {
					parseException = e;
				}
				if ((statsStoreCli != null) && statsStoreCli.isCommandLineChange()) {
					commandLine = statsStoreCli.getCommandLine();
					newCommandLine = true;
					continue;
				}
				statsOptions = StatsCommandLineOptions.parseOptions(commandLine);
			}
			while (newCommandLine);
			if (parseException != null) {
				throw parseException;
			}

			final String[] authorizations = getAuthorizations(statsOptions.getAuthorizations());
			final AdapterStore adapterStore = adapterStoreCli.getResult().createStore();
			final DataStore dataStore = dataStoreCli.getResult().createStore();
			final IndexStore indexStore = indexStoreCli.getResult().createStore();
			final DataStatisticsStore statsStore = statsStoreCli.getResult().createStore();
			if (statsOptions.getAdapterId() != null) {
				final ByteArrayId adapterId = new ByteArrayId(
						statsOptions.getAdapterId());
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
					return false;
				}
				return calculateStatistics(
						dataStore,
						indexStore,
						statsStore,
						adapter,
						authorizations);
			}
			else {
				boolean success = false;
				try (CloseableIterator<DataAdapter<?>> adapterIt = adapterStore.getAdapters()) {
					if (adapterIt.hasNext()) {
						success = true;
					}
					while (adapterIt.hasNext()) {
						final DataAdapter<?> adapter = adapterIt.next();
						if (!calculateStatistics(
								dataStore,
								indexStore,
								statsStore,
								adapter,
								authorizations)) {
							success = false;
							LOGGER.info("Unable to calculate statistics for adapter: " + adapter.getAdapterId().getString());
						}
					}
				}
				return success;
			}
		}
		catch (final ParseException e) {
			LOGGER.error(
					"Unable to parse stats tool arguments",
					e);
			return false;
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to parse stats tool arguments",
					e);
			return false;
		}

	}

	// protected static boolean runForAdapter(
	// final DataAdapt adapterId,
	// final AdapterStore adapterStore,
	// final DataStore dataStore,
	// final IndexStore indexStore,
	// final DataStatisticsStore statsStore,
	// final String[] authorizations ) {
	// }
}
