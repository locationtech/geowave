package mil.nga.giat.geowave.datastore.accumulo.cli;

import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;

/**
 * 
 * Simple command line tool to print statistics to the standard output
 * 
 */
public class DumpStatsOperation extends
		StatsOperation implements
		CLIOperationDriver
{
	public boolean doWork(
			AccumuloDataStatisticsStore statsStore,
			DataStore dataStore,
			IndexStore indexStore,
			DataAdapter<?> adapter,
			String[] authorizations ) {
		try (CloseableIterator<DataStatistics<?>> statsIt = statsStore.getAllDataStatistics(authorizations)) {
			while (statsIt.hasNext()) {
				final DataStatistics<?> stats = statsIt.next();
				if (adapter != null && !adapter.getAdapterId().equals(
						stats.getDataAdapterId())) continue;
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

}
