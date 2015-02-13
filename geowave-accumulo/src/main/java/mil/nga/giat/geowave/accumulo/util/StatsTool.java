package mil.nga.giat.geowave.accumulo.util;

import java.io.IOException;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.AccumuloOptions;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.statistics.StatsCompositionTool;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.query.Query;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;

/**
 * 
 * Simple command line tool to recalculate statistics for an adapter. Command
 * line options must provided in order:
 * 
 * zookeeper-host:port accumulo-instance-name user password namespace adapterId
 * comma-separated-authorizations
 * 
 */
public class StatsTool
{

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
			System.err.println("Unknown adapter " + adapterId);
			return false;
		}
		statsStore.deleteObjects(
				adapter.getAdapterId(),
				authorizations);
		final StatsCompositionTool<?> statsTool = new StatsCompositionTool(
				adapter,
				statsStore);
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
		statsTool.flush();
		return true;
	}

	public static void main(
			final String args[] )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		final String zookeeper = args[0];
		final String accumuloInstance = args[1];
		final String accumuloUser = args[2];
		final String accumuloPassword = args[3];
		final String namespace = args[4];
		final String adapterId = args[5];
		final String authorizations = args.length > 6 ? args[6] : null;
		final AccumuloOperations accumuloOperations = new BasicAccumuloOperations(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword,
				namespace);
		System.exit(calculateStastics(
				accumuloOperations,
				new ByteArrayId(
						adapterId),
				getAuthorizations(authorizations)) ? 0 : -1);

	}

	private static String[] getAuthorizations(
			final String auths ) {
		if (auths == null) return new String[0];
		final String[] authsArray = auths.split(",");
		for (int i = 0; i < authsArray.length; i++) {
			authsArray[i] = authsArray[i].trim();
		}
		return authsArray;
	}
}
