package mil.nga.giat.geowave.cli.stats;

import java.io.IOException;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;

import org.apache.log4j.Logger;

public class DumpStatsOperation extends
		AbstractStatsOperation
{
	private static final Logger LOGGER = Logger.getLogger(DumpStatsOperation.class);

	@Override
	protected boolean calculateStatistics(
			final DataStore dataStore,
			final IndexStore indexStore,
			final DataStatisticsStore statsStore,
			final DataAdapter<?> adapter,
			final String[] authorizations )
			throws IOException {
		try (CloseableIterator<DataStatistics<?>> statsIt = statsStore.getAllDataStatistics(authorizations)) {
			while (statsIt.hasNext()) {
				final DataStatistics<?> stats = statsIt.next();
				if ((adapter != null) && !stats.getDataAdapterId().equals(
						adapter.getAdapterId())) {
					continue;
				}
				try {
					System.out.println(stats.toString());
				}
				catch (final Exception ex) {
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
