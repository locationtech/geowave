/**
 * 
 */
package mil.nga.giat.geowave.datastore.cli;

import java.io.IOException;

import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.datastore.hbase.HBaseAdapterStore;
import mil.nga.giat.geowave.datastore.hbase.HBaseCommandLineOptions;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStore;
import mil.nga.giat.geowave.datastore.hbase.HBaseIndexStore;
import mil.nga.giat.geowave.datastore.hbase.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to <code> DumpStatsOperation </code>
 */
public class HBaseDumpStatsOperation implements
		CLIOperationDriver
{

	protected static final Logger LOGGER = Logger.getLogger(StatsOperationCLIProvider.class);

	@Override
	public void run(
			final String[] args )
			throws ParseException {
		try {
			final Options allOptions = new Options();
			HBaseCommandLineOptions.applyOptions(allOptions);
			StatsCommandLineOptions.applyOptions(
					allOptions,
					isTypeRequired());
			final BasicParser parser = new BasicParser();
			try {
				final CommandLine commandLine = parser.parse(
						allOptions,
						args);
				HBaseCommandLineOptions options;
				options = HBaseCommandLineOptions.parseOptions(commandLine);

				final StatsCommandLineOptions statsOperations = StatsCommandLineOptions.parseOptions(commandLine);
				runOperation(
						options.getOperations(),
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
		catch (IOException e) {
			LOGGER.error(
					"Error while calculating statistics.",
					e);
		}
	}

	public boolean runOperation(
			final BasicHBaseOperations operations,
			final ByteArrayId adapterId,
			final String[] authorizations )
			throws IOException {
		final HBaseOptions options = new HBaseOptions();
		options.setPersistDataStatistics(true);
		final HBaseDataStore dataStore = new HBaseDataStore(
				operations,
				options);
		final HBaseAdapterStore adapterStore = new HBaseAdapterStore(
				operations);
		final HBaseIndexStore indexStore = new HBaseIndexStore(
				operations);
		final HBaseDataStatisticsStore statsStore = new HBaseDataStatisticsStore(
				operations);
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

	public boolean doWork(
			HBaseDataStatisticsStore statsStore,
			DataStore dataStore,
			IndexStore indexStore,
			DataAdapter<?> adapter,
			String[] authorizations ) {
		try (CloseableIterator<DataStatistics<?>> statsIt = statsStore.getAllDataStatistics(authorizations)) {
			while (statsIt.hasNext()) {
				final DataStatistics<?> stats = statsIt.next();
				if (adapter != null && !stats.getDataAdapterId().equals(
						adapter.getAdapterId())) continue;
				try {
					System.out.println(stats.toString());
				}
				catch (Exception ex) {
					LOGGER.error(
							"Malformed statistic",
							ex);
				}
			}
		}
		catch (final Exception ex) {
			LOGGER.error(
					"Error while dumping statistics.",
					ex);
			return false;
		}
		return true;
	}

	@Override
	public void runOperation(
			String[] args )
			throws ParseException {
		try {
			final Options allOptions = new Options();
			HBaseCommandLineOptions.applyOptions(allOptions);
			StatsCommandLineOptions.applyOptions(
					allOptions,
					isTypeRequired());
			final BasicParser parser = new BasicParser();
			try {
				final CommandLine commandLine = parser.parse(
						allOptions,
						args);
				HBaseCommandLineOptions options;
				options = HBaseCommandLineOptions.parseOptions(commandLine);

				final StatsCommandLineOptions statsOperations = StatsCommandLineOptions.parseOptions(commandLine);
				runOperation(
						options.getOperations(),
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
		catch (IOException e) {
			LOGGER.error(
					"Error while calculating statistics.",
					e);
		}
	}
}
