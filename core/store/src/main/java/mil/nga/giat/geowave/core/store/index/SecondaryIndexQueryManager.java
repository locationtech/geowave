package mil.nga.giat.geowave.core.store.index;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.query.BasicQuery;

/**
 * Manages query the secondary indices given a query. Eventually is replaced by
 * a CBO!
 * 
 * 
 * @param <T>
 *            The type of entity being indexed
 */
public class SecondaryIndexQueryManager
{
	final SecondaryIndexDataStore secondaryIndexDataStore;

	public SecondaryIndexQueryManager(
			final SecondaryIndexDataStore secondaryIndexDataStore ) {
		this.secondaryIndexDataStore = secondaryIndexDataStore;
	}

	/**
	 * 
	 * @param query
	 * @param secondaryIndex
	 * @param primaryIndex
	 * @param visibility
	 * @return
	 */
	public CloseableIterator<ByteArrayId> query(
			final BasicQuery query,
			final SecondaryIndex<?> secondaryIndex,
			final PrimaryIndex primaryIndex,
			final String... visibility ) {
		if (query.isSupported(secondaryIndex)) {
			return secondaryIndexDataStore.query(
					secondaryIndex,
					query.getSecondaryIndexConstraints(secondaryIndex),
					query.getSecondaryQueryFilter(secondaryIndex),
					primaryIndex.getId(),
					visibility);
		}
		return new CloseableIterator.Empty<ByteArrayId>();
	}

}
