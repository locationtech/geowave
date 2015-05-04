package mil.nga.giat.geowave.datastore.accumulo.metadata;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig;
import mil.nga.giat.geowave.datastore.accumulo.MergingCombiner;
import mil.nga.giat.geowave.datastore.accumulo.MergingVisibilityCombiner;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.hadoop.io.Text;

/**
 * This class will persist Index objects within an Accumulo table for GeoWave
 * metadata. The adapters will be persisted in an "INDEX" column family.
 * 
 * There is an LRU cache associated with it so staying in sync with external
 * updates is not practical - it assumes the objects are not updated often or at
 * all. The objects are stored in their own table.
 * 
 **/
public class AccumuloDataStatisticsStore extends
		AbstractAccumuloPersistence<DataStatistics<?>> implements
		DataStatisticsStore
{
	// this is fairly arbitrary at the moment because it is the only custom
	// iterator added
	private static final int STATS_COMBINER_PRIORITY = 10;
	private static final int STATS_MULTI_VISIBILITY_COMBINER_PRIORITY = 15;
	private static final String STATISTICS_CF = "STATS";

	public AccumuloDataStatisticsStore(
			final AccumuloOperations accumuloOperations ) {
		super(
				accumuloOperations);
	}

	@Override
	public void incorporateStatistics(
			final DataStatistics<?> statistics ) {
		// because we're using the combiner, we should simply be able to add the
		// object
		addObject(statistics);

		// TODO if we do allow caching after we add a statistic to Accumulo we
		// do need to make sure we update our cache, but for now we aren't using
		// the cache at all

	}

	@Override
	protected void addObjectToCache(
			final DataStatistics<?> object ) {
		// don't use the cache at all for now

		// TODO consider adding a setting to use the cache for statistics, but
		// because it could change with each new entry, it seems that there
		// could be too much potential for invalid caching if multiple instances
		// of GeoWave are able to connect to the same Accumulo tables
	}

	@Override
	protected Object getObjectFromCache(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {
		// don't use the cache at all

		// TODO consider adding a setting to use the cache for statistics, but
		// because it could change with each new entry, it seems that there
		// could be too much potential for invalid caching if multiple instances
		// of GeoWave are able to connect to the same Accumulo tables
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
		// of GeoWave are able to connect to the same Accumulo tables
		return true;
	}

	@Override
	protected IteratorConfig[] getIteratorConfig() {
		final IteratorConfig statsCombiner = new IteratorConfig(
				new IteratorSetting(
						STATS_COMBINER_PRIORITY,
						MergingCombiner.class),
				EnumSet.allOf(IteratorScope.class));
		final List<Column> columns = new ArrayList<Column>();
		columns.add(new Column(
				STATISTICS_CF));
		Combiner.setColumns(
				statsCombiner.getIteratorSettings(),
				columns);
		return new IteratorConfig[] {
			statsCombiner
		};
	}

	@Override
	protected IteratorSetting[] getScanSettings() {
		final IteratorSetting statsMultiVisibilityCombiner = new IteratorSetting(
				STATS_MULTI_VISIBILITY_COMBINER_PRIORITY,
				MergingVisibilityCombiner.class);
		return new IteratorSetting[] {
			statsMultiVisibilityCombiner
		};
	}

	@Override
	public DataStatistics<?> getDataStatistics(
			final ByteArrayId adapterId,
			final ByteArrayId statisticsId,
			String... authorizations ) {
		return getObject(
				statisticsId,
				adapterId,
				authorizations);
	}

	@Override
	protected DataStatistics<?> entryToValue(
			final Entry<Key, Value> entry ) {
		final DataStatistics<?> stats = super.entryToValue(entry);
		if (stats != null) {
			stats.setDataAdapterId(getSecondaryId(entry.getKey()));
			final Text visibility = entry.getKey().getColumnVisibility();
			if (visibility != null) {
				stats.setVisibility(visibility.getBytes());
			}
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
			String... authorizations ) {
		return getObjects(authorizations);
	}

	@Override
	public boolean removeStatistics(
			final ByteArrayId adapterId,
			final ByteArrayId statisticsId,
			String... authorizations ) {
		return deleteObject(
				statisticsId,
				adapterId,
				authorizations);
	}

	@Override
	public CloseableIterator<DataStatistics<?>> getDataStatistics(
			final ByteArrayId adapterId,
			String... authorizations ) {
		return getAllObjectsWithSecondaryId(
				adapterId,
				authorizations);
	}

	@Override
	protected String getPersistenceTypeName() {
		return STATISTICS_CF;
	}

	@Override
	protected byte[] getAccumuloVisibility(
			final DataStatistics<?> entry ) {
		return entry.getVisibility();
	}
}
