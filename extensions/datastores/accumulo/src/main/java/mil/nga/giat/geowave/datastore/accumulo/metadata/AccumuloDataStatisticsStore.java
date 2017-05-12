package mil.nga.giat.geowave.datastore.accumulo.metadata;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.BasicOptionProvider;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig;
import mil.nga.giat.geowave.datastore.accumulo.MergingCombiner;
import mil.nga.giat.geowave.datastore.accumulo.MergingVisibilityCombiner;
import mil.nga.giat.geowave.datastore.accumulo.util.TransformerWriter;
import mil.nga.giat.geowave.datastore.accumulo.util.VisibilityTransformer;

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
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloDataStatisticsStore.class);
	// this is fairly arbitrary at the moment because it is the only custom
	// iterator added
	private static final int STATS_COMBINER_PRIORITY = 10;
	private static final int STATS_MULTI_VISIBILITY_COMBINER_PRIORITY = 15;
	private static final String STATISTICS_COMBINER_NAME = "STATS_COMBINER";
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
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
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
		final Column adapterColumn = new Column(
				STATISTICS_CF);
		final Map<String, String> options = new HashMap<String, String>();
		options.put(
				MergingCombiner.COLUMNS_OPTION,
				ColumnSet.encodeColumns(
						adapterColumn.getFirst(),
						adapterColumn.getSecond()));
		final IteratorConfig statsCombiner = new IteratorConfig(
				EnumSet.allOf(IteratorScope.class),
				STATS_COMBINER_PRIORITY,
				STATISTICS_COMBINER_NAME,
				MergingCombiner.class.getName(),
				new BasicOptionProvider(
						options));
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
			final String... authorizations ) {
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

	@Override
	public void transformVisibility(
			final ByteArrayId adapterId,
			final String transformingRegex,
			final String replacement,
			final String... authorizations ) {
		Scanner scanner;

		try {
			scanner = createSortScanner(
					adapterId,
					authorizations);

			final TransformerWriter writer = new TransformerWriter(
					scanner,
					getTablename(),
					accumuloOperations,
					new VisibilityTransformer(
							transformingRegex,
							replacement));
			writer.transform();
			scanner.close();
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Table not found during transaction commit: " + getTablename(),
					e);
		}
	}

	private Scanner createSortScanner(
			final ByteArrayId adapterId,
			final String... authorizations )
			throws TableNotFoundException {
		Scanner scanner = null;

		scanner = accumuloOperations.createScanner(
				getTablename(),
				authorizations);

		final IteratorSetting[] settings = getScanSettings();
		if ((settings != null) && (settings.length > 0)) {
			for (final IteratorSetting setting : settings) {
				scanner.addScanIterator(setting);
			}
		}
		final String columnFamily = getColumnFamily();
		final String columnQualifier = getColumnQualifier(adapterId);
		scanner.fetchColumn(
				new Text(
						columnFamily),
				new Text(
						columnQualifier));

		// scanner.setRange(Range.);
		return scanner;
	}
}
