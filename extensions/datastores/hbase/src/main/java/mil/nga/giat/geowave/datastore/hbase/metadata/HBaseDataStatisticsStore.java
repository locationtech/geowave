package mil.nga.giat.geowave.datastore.hbase.metadata;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

public class HBaseDataStatisticsStore extends
		AbstractHBasePersistence<DataStatistics<?>> implements
		DataStatisticsStore
{

	protected static final String STATISTICS_CF = "STATS";
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseDataStatisticsStore.class);

	public HBaseDataStatisticsStore(
			final BasicHBaseOperations operations ) {
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
			LOGGER.error("No statistics id specified for removeStatistics, ignoring request!");
			return false;
		}
		return deleteObject(
				statisticsId,
				adapterId,
				authorizations);
	}

	@Override
	protected ByteArrayId getPrimaryId(
			final DataStatistics<?> persistedObject ) {
		return persistedObject.getStatisticsId();
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

	@Override
	public void transformVisibility(
			final ByteArrayId adapterId,
			final String transformingRegex,
			final String replacement,
			final String... authorizations ) {
		// TODO Unimplemented
		LOGGER.error("This method transformVisibility is not yet coded. Need to fix it");

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
				0
			});
			// So this will set the stop row to just after all the possible
			// suffixes to this primaryId.
			scan.setStopRow(HBaseUtils.getNextPrefix(buf.array()));
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

				// This entryToValue function has the side effect of adding the
				// object to the cache.
				// We need to make sure to add the merged version of the stat at
				// the end of this
				// function, before it is returned.
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

	/**
	 * This function will append a UUID to the record that's inserted into the
	 * database.
	 *
	 * @param object
	 * @return
	 */
	@Override
	protected ByteArrayId getRowId(
			final DataStatistics<?> object ) {
		final byte[] parentRecord = super.getRowId(
				object).getBytes();
		return HBaseUtils.ensureUniqueId(
				parentRecord,
				false);
	}
}
