package mil.nga.giat.geowave.cli.stats;

import java.io.IOException;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStoreStatsAdapterWrapper;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatsCompositionTool;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

/**
 * 
 * Simple command line tool to recalculate statistics for an adapter.
 * 
 */
public class StatsOperation extends
		AbstractStatsOperation
{
	private static final Logger LOGGER = Logger.getLogger(StatsOperation.class);

	@Override
	protected boolean calculateStatistics(
			final DataStore dataStore,
			final IndexStore indexStore,
			final DataStatisticsStore statsStore,
			final DataAdapter<?> adapter,
			final String[] authorizations )
			throws IOException {
		statsStore.removeAllStatistics(
				adapter.getAdapterId(),
				authorizations);

		try (CloseableIterator<Index<?, ?>> indexit = indexStore.getIndices()) {
			while (indexit.hasNext()) {
				final PrimaryIndex index = (PrimaryIndex) indexit.next();
				try (StatsCompositionTool<?> statsTool = new StatsCompositionTool(
						new DataStoreStatsAdapterWrapper(
								index,
								(WritableDataAdapter) adapter),
						statsStore)) {
					try (CloseableIterator<?> entryIt = dataStore.query(
							new QueryOptions(
									adapter,
									index,
									(Integer) null,
									statsTool,
									authorizations),
							(Query) null)) {
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
}
