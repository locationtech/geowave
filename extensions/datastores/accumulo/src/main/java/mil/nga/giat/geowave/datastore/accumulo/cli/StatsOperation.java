package mil.nga.giat.geowave.datastore.accumulo.cli;

import java.io.IOException;

import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloCommandLineOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * 
 * Simple command line tool to recalculate statistics for an adapter.
 * 
 */
public abstract class StatsOperation implements
		CLIOperationDriver
{
	protected static final Logger LOGGER = Logger.getLogger(StatsOperation.class);

	public boolean runOperation(
			final AccumuloOperations accumuloOperations,
			final ByteArrayId adapterId,
			final String[] authorizations )
			throws IOException {
		final AccumuloOptions accumuloOptions = new AccumuloOptions();
		accumuloOptions.setPersistDataStatistics(true);
		final AccumuloDataStore dataStore = new AccumuloDataStore(
				accumuloOperations,
				accumuloOptions);
		final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				accumuloOperations);
		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				accumuloOperations);
		final AccumuloDataStatisticsStore statsStore = new AccumuloDataStatisticsStore(
				accumuloOperations);
		DataAdapter<?> adapter = null;
		if (adapterId != null) {
			adapterStore.getAdapter(adapterId);
			if (adapter == null) {
				LOGGER.error("Unknown adapter " + adapterId);
				return false;
			}
		}
		return doWork(
				statsStore,
				dataStore,
				indexStore,
				adapter,
				authorizations);
	}

	public abstract boolean doWork(
			AccumuloDataStatisticsStore statsStore,
			DataStore dataStore,
			IndexStore indexStore,
			DataAdapter<?> adapter,
			String[] authorizations );

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

	/** Is an adapter type/id required? */
	protected boolean isTypeRequired() {
		return false;
	}

	@Override
	public void run(
			final String[] args )
			throws ParseException {
		try {
			final Options allOptions = new Options();
			AccumuloCommandLineOptions.applyOptions(allOptions);
			StatsCommandLineOptions.applyOptions(
					allOptions,
					isTypeRequired());
			final BasicParser parser = new BasicParser();
			try {
				final CommandLine commandLine = parser.parse(
						allOptions,
						args);
				AccumuloCommandLineOptions accumuloOperations;
				accumuloOperations = AccumuloCommandLineOptions.parseOptions(commandLine);

				final StatsCommandLineOptions statsOperations = StatsCommandLineOptions.parseOptions(commandLine);
				runOperation(
						accumuloOperations.getAccumuloOperations(),
						statsOperations.getTypeName() != null ? new ByteArrayId(
								statsOperations.getTypeName()) : null,
						getAuthorizations(statsOperations.getAuthorizations()));
			}
			catch (final ParseException e) {
				LOGGER.error(
						"Unable to parse stats tool arguments",
						e);
			}
		}
		catch (AccumuloException | AccumuloSecurityException | IOException e) {
			LOGGER.error(
					"Error while calculating statistics.",
					e);
		}
	}

}
