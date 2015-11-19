package mil.nga.giat.geowave.adapter.vector.plugin.transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

public abstract class AbstractTransactionManagement implements
		GeoWaveTransaction
{

	protected final GeoWaveDataStoreComponents components;

	public AbstractTransactionManagement(
			GeoWaveDataStoreComponents components ) {
		super();
		this.components = components;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<ByteArrayId, DataStatistics<SimpleFeature>> getDataStatistics() {
		final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats = new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>();
		final FeatureDataAdapter adapter = components.getAdapter();
		ByteArrayId[] ids = adapter.getSupportedStatisticsIds();
		Set<ByteArrayId> idSet = new HashSet<ByteArrayId>();
		for (ByteArrayId id : ids)
			idSet.add(id);
		try (CloseableIterator<DataStatistics<?>> it = components.getDataStore().getStatsStore().getDataStatistics(
				adapter.getAdapterId(),
				composeAuthorizations())) {
			while (it.hasNext()) {
				DataStatistics<?> stat = (DataStatistics<?>) it.next();
				if (idSet.contains(stat.getStatisticsId())) stats.put(
						stat.getStatisticsId(),
						(DataStatistics<SimpleFeature>) stat);
			}

		}
		catch (Exception e) {
			GeoWaveTransactionManagement.LOGGER.error(
					"Failed to access statistics from data store",
					e);
		}
		return stats;
	}

}
