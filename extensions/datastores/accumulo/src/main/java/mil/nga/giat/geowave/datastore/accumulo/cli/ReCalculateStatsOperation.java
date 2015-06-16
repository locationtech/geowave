package mil.nga.giat.geowave.datastore.accumulo.cli;

import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatsCompositionTool;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;

/**
 * 
 * Simple command line tool to recalculate statistics for an adapter.
 * 
 */
public class ReCalculateStatsOperation extends
		StatsOperation implements
		CLIOperationDriver
{

	/** Is an adapter type/id required? */
	@Override
	protected boolean isTypeRequired() {
		return true;
	}

	@Override
	public boolean doWork(
			AccumuloDataStatisticsStore statsStore,
			DataStore dataStore,
			IndexStore indexStore,
			DataAdapter<?> adapter,
			String[] authorizations ) {
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

}
