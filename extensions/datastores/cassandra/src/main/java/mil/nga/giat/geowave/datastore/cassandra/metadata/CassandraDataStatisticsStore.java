package mil.nga.giat.geowave.datastore.cassandra.metadata;

import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Select;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;

public class CassandraDataStatisticsStore extends
		AbstractCassandraPersistence<DataStatistics<?>> implements
		DataStatisticsStore
{

	protected static final String STATISTICS_CF = "STATS";
	private final static Logger LOGGER = Logger.getLogger(CassandraDataStatisticsStore.class);

	public CassandraDataStatisticsStore(
			final CassandraOperations operations ) {
		super(
				operations);
	}

	@Override
	public void incorporateStatistics(
			final DataStatistics<?> statistics ) {
		// because we're using the combiner, we should simply be able to add the
		// object
		addObject(statistics);

		// TODO if we do allow caching after we add a statistic to Cassandra we
		// do need to make sure we update our cache, but for now we aren't using
		// the cache at all

	}

	@Override
	protected void addObjectToCache(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final DataStatistics<?> object ) {
		// don't use the cache at all for now

		// TODO consider adding a setting to use the cache for statistics, but
		// because it could change with each new entry, it seems that there
		// could be too much potential for invalid caching if multiple instances
		// of GeoWave are able to connect to the same Cassandra tables
	}

	@Override
	protected Object getObjectFromCache(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {
		// don't use the cache at all

		// TODO consider adding a setting to use the cache for statistics, but
		// because it could change with each new entry, it seems that there
		// could be too much potential for invalid caching if multiple instances
		// of GeoWave are able to connect to the same Cassandra tables
		return null;
	}

	@Override
	protected boolean deleteObjectFromCache(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {
		// don't use the cache at all

		// TODO consider adding a setting to use the cache for statistics, but
		// because it could change with each new entry, it seems that there
		// could be too much potential for invalid caching if multiple instances
		// of GeoWave are able to connect to the same Cassandra tables
		return true;
	}

	@Override
	public DataStatistics<?> getDataStatistics(
			final ByteArrayId adapterId,
			final ByteArrayId statisticsId,
			final String... authorizations ) {
		return getObject(
				statisticsId,
				adapterId,
				authorizations);
	}

	@Override
	protected Select getSelect() {
		return operations.getSelect(
				getTablename(),
				SECONDARY_ID_KEY,
				VALUE_KEY);
	}

	@Override
	protected DataStatistics<?> entryToValue(
			final Row entry ) {
		final DataStatistics<?> stats = super.entryToValue(entry);
		if (stats != null) {
			stats.setDataAdapterId(getSecondaryId(entry));
		}
		return stats;
	}

	@Override
	protected ByteArrayId getPrimaryId(
			final DataStatistics<?> persistedObject ) {
		return persistedObject.getStatisticsId();
	}

	@Override
	protected ByteArrayId getSecondaryId(
			final DataStatistics<?> persistedObject ) {
		return persistedObject.getDataAdapterId();
	}

	@Override
	public void setStatistics(
			final DataStatistics<?> statistics ) {
		removeStatistics(
				statistics.getDataAdapterId(),
				statistics.getStatisticsId());
		addObject(statistics);
	}

	@Override
	public CloseableIterator<DataStatistics<?>> getAllDataStatistics(
			final String... authorizations ) {
		return getObjects(authorizations);
	}

	@Override
	public boolean removeStatistics(
			final ByteArrayId adapterId,
			final ByteArrayId statisticsId,
			final String... authorizations ) {
		return deleteObject(
				statisticsId,
				adapterId,
				authorizations);
	}

	@Override
	public CloseableIterator<DataStatistics<?>> getDataStatistics(
			final ByteArrayId adapterId,
			final String... authorizations ) {
		return getAllObjectsWithSecondaryId(
				adapterId,
				authorizations);
	}

	@Override
	protected boolean hasSecondaryId() {
		return true;
	}

	@Override
	protected String getPersistenceTypeName() {
		return STATISTICS_CF;
	}

	@Override
	protected byte[] getVisibility(
			final DataStatistics<?> entry ) {
		return entry.getVisibility();
	}

	@Override
	public void removeAllStatistics(
			final ByteArrayId adapterId,
			final String... authorizations ) {
		deleteObjects(
				adapterId,
				authorizations);
	}

	/**
	 * This function converts results and merges data statistic elements
	 * together that have the same id.
	 */
	@Override
	protected Iterator<DataStatistics<?>> getNativeIteratorWrapper(
			final Iterator<Row> results ) {
		return new StatisticsNativeIteratorWrapper(
				results);
	}

	/**
	 * A special version of NativeIteratorWrapper (defined in the parent) which
	 * will combine records that have the same dataid & statsId
	 */
	private class StatisticsNativeIteratorWrapper implements
			Iterator<DataStatistics<?>>
	{
		final private Iterator<Row> it;
		private DataStatistics<?> nextVal = null;

		public StatisticsNativeIteratorWrapper(
				final Iterator<Row> resultIterator ) {
			it = resultIterator;
		}

		@Override
		public boolean hasNext() {
			return (nextVal != null) || it.hasNext();
		}

		@Override
		public DataStatistics<?> next() {
			DataStatistics<?> currentStatistics = nextVal;
			nextVal = null;
			while (it.hasNext()) {
				final Row row = it.next();

				// This entryToValue function has the side effect of adding the
				// object to the cache.
				// We need to make sure to add the merged version of the stat at
				// the end of this
				// function, before it is returned.
				final DataStatistics<?> statEntry = entryToValue(row);

				if (currentStatistics == null) {
					currentStatistics = statEntry;
				}
				else {
					if (statEntry.getStatisticsId().equals(
							currentStatistics.getStatisticsId()) && statEntry.getDataAdapterId().equals(
							currentStatistics.getDataAdapterId())) {
						currentStatistics.merge(statEntry);
					}
					else {
						nextVal = statEntry;
						break;
					}
				}
			}

			// Add this entry to cache (see comment above)
			addObjectToCache(
					getPrimaryId(currentStatistics),
					getSecondaryId(currentStatistics),
					currentStatistics);
			return currentStatistics;
		}

		@Override
		public void remove() {
			throw new NotImplementedException(
					"Transforming iterator cannot use remove()");
		}

	}
}
