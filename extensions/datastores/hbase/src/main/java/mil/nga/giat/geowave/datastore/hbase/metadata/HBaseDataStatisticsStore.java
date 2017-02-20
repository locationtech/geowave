package mil.nga.giat.geowave.datastore.hbase.metadata;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.hbase.operations.HBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

public class HBaseDataStatisticsStore extends
		AbstractHBasePersistence<DataStatistics<?>> implements
		DataStatisticsStore
{

	protected static final String STATISTICS_CF = "STATS";
	private final static Logger LOGGER = Logger.getLogger(HBaseDataStatisticsStore.class);

	public HBaseDataStatisticsStore(
			final HBaseOperations operations ) {
		super(
				operations);
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
	public void incorporateStatistics(
			final DataStatistics<?> statistics ) {
		addObject(statistics);

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
	public CloseableIterator<DataStatistics<?>> getAllDataStatistics(
			final String... authorizations ) {
		return getObjects(authorizations);
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
	public boolean removeStatistics(
			final ByteArrayId adapterId,
			final ByteArrayId statisticsId,
			final String... authorizations ) {
		if (statisticsId == null) {
			LOGGER.log(
					Level.ERROR,
					"No statistics id specified for removeStatistics, ignoring request!");
			return false;
		}
		return deleteObject(
				statisticsId,
				adapterId,
				authorizations);
	}

	/**
	 * This function will append a UUID to the record that's inserted into the
	 * database.
	 */
	@Override
	protected ByteArrayId getPrimaryId(
			final DataStatistics<?> persistedObject ) {
		final byte[] parentRecord = persistedObject.getStatisticsId().getBytes();
		return DataStoreUtils.ensureUniqueId(
				parentRecord,
				false);
	}

	@Override
	protected String getPersistenceTypeName() {
		return STATISTICS_CF;
	}

	@Override
	protected ByteArrayId getSecondaryId(
			final DataStatistics<?> persistedObject ) {
		return persistedObject.getDataAdapterId();
	}

	@Override
	protected DataStatistics<?> entryToValue(
			final Cell entry ) {
		final DataStatistics<?> stats = super.entryToValue(entry);
		if (stats != null) {
			stats.setDataAdapterId(new ByteArrayId(
					CellUtil.cloneQualifier(entry)));
		}
		return stats;
	}

	@Override
	public void removeAllStatistics(
			final ByteArrayId adapterId,
			final String... authorizations ) {
		deleteObjects(
				null,
				adapterId,
				authorizations);

	}

	/**
	 * This function is used to change the scan object created in the superclass
	 * to enable prefixing.
	 */
	@Override
	protected Scan applyScannerSettings(
			final Scan scanner,
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {
		final Scan scan = super.applyScannerSettings(
				scanner,
				primaryId,
				secondaryId);
		if (primaryId != null) {
			final ByteBuffer buf = ByteBuffer.allocate(primaryId.getBytes().length + 1);
			buf.put(primaryId.getBytes());
			buf.put(new byte[] {
				DataStoreUtils.UNIQUE_ID_DELIMITER
			});
			// So this will set the stop row to just after all the possible
			// suffixes to this primaryId.
			scan.setStopRow(new ByteArrayId(
					buf.array()).getNextPrefix());
		}
		return scan;
	}

	/**
	 * This function converts results and merges data statistic elements
	 * together that have the same id.
	 */
	@Override
	protected Iterator<DataStatistics<?>> getNativeIteratorWrapper(
			final Iterator<Result> resultIterator ) {
		return new StatisticsNativeIteratorWrapper(
				resultIterator);
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
		// of GeoWave are able to connect to the same HBase tables
	}

	@Override
	protected Object getObjectFromCache(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {
		// don't use the cache at all

		// TODO consider adding a setting to use the cache for statistics, but
		// because it could change with each new entry, it seems that there
		// could be too much potential for invalid caching if multiple instances
		// of GeoWave are able to connect to the same HBase tables
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
		// of GeoWave are able to connect to the same HBase tables
		return true;
	}

	/**
	 * A special version of NativeIteratorWrapper (defined in the parent) which
	 * will combine records that have the same dataid & statsId
	 */
	private class StatisticsNativeIteratorWrapper implements
			Iterator<DataStatistics<?>>
	{
		final private Iterator<Result> it;
		private DataStatistics<?> nextVal = null;

		public StatisticsNativeIteratorWrapper(
				final Iterator<Result> resultIterator ) {
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
				final Cell cell = it.next().listCells().get(
						0);
				final DataStatistics<?> statEntry = entryToValue(cell);

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
			return currentStatistics;
		}

		@Override
		public void remove() {
			throw new NotImplementedException(
					"Transforming iterator cannot use remove()");
		}

	}
}
