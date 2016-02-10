/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase.metadata;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

/**
 * @author viggy Functionality similar to
 *         <code> AccumuloDataStatisticsStore </code>
 */
public class HBaseDataStatisticsStore extends
		AbstractHBasePersistence<DataStatistics<?>> implements
		DataStatisticsStore
{

	private static final String STATISTICS_CF = "STATS";
	private final static Logger LOGGER = Logger.getLogger(HBaseDataStatisticsStore.class);

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
			stats.setDataAdapterId(new ByteArrayId(CellUtil.cloneQualifier(entry)));
		}
		return stats;
	}

	@Override
	public void removeAllStatistics(
			final ByteArrayId adapterId,
			final String... authorizations ) {
		// TODO #406 Need to fix
		LOGGER.error("This method removeAllStatistics is not yet coded. Need to fix it");

	}

	@Override
	public void transformVisibility(
			final ByteArrayId adapterId,
			final String transformingRegex,
			final String replacement,
			final String... authorizations ) {
		// TODO #406 Need to fix
		LOGGER.error("This method transformVisibility is not yet coded. Need to fix it");

	}

}
