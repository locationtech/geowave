package mil.nga.giat.geowave.datastore.accumulo.util;

import java.io.IOException;

import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatsCompositionTool;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.Query;
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
public class StatsOperation implements
		CLIOperationDriver
{
	private static final Logger LOGGER = Logger.getLogger(StatsOperationCLIProvider.class);

	public static boolean calculateStastics(
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
		final DataAdapter<?> adapter = adapterStore.getAdapter(adapterId);
		if (adapter == null) {
			LOGGER.error("Unknown adapter " + adapterId);
			return false;
		}
		statsStore.deleteObjects(
				adapter.getAdapterId(),
				authorizations);
		try (StatsCompositionTool<?> statsTool = new StatsCompositionTool(
				adapter,
				statsStore)) {
			try (CloseableIterator<Index> indexit = indexStore.getIndices()) {
				while (indexit.hasNext()) {
					final Index index = indexit.next();
					try (CloseableIterator<?> entryIt = dataStore.query(
							adapter,
							index,
							(Query) null,
							(Integer) null,
							statsTool,
							authorizations)) {
						while (entryIt.hasNext()) {
							entryIt.next();
						}
					}
				}
			}
		}
		catch (final Exception ex) {
			LOGGER.error(
					"Error while writing statistics.",
					ex);
			return false;
		}
		return true;
	}

	public static void main(
			final String args[] )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		final Options allOptions = new Options();
		AccumuloCommandLineOptions.applyOptions(allOptions);
		StatsCommandLineOptions.applyOptions(allOptions);
		final BasicParser parser = new BasicParser();
		try {
			final CommandLine commandLine = parser.parse(
					allOptions,
					args);
			AccumuloCommandLineOptions accumuloOperations;
			accumuloOperations = AccumuloCommandLineOptions.parseOptions(commandLine);

			final StatsCommandLineOptions statsOperations = StatsCommandLineOptions.parseOptions(commandLine);
			System.exit(calculateStastics(
					accumuloOperations.getAccumuloOperations(),
					new ByteArrayId(
							statsOperations.getTypeName()),
					getAuthorizations(statsOperations.getAuthorizations())) ? 0 : -1);
		}
		catch (final ParseException e) {
			LOGGER.error(
					"Unable to parse stats tool arguments",
					e);
		}

	}

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
	public void run(
			final String[] args )
			throws ParseException {
		try {
			main(args);
		}
		catch (AccumuloException | AccumuloSecurityException | IOException e) {
			LOGGER.error(
					"Error while calculating statistics.",
					e);
		}
	}

}
