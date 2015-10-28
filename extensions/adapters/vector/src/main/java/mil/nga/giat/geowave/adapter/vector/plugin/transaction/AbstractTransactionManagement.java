package mil.nga.giat.geowave.adapter.vector.plugin.transaction;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

import org.opengis.feature.simple.SimpleFeature;

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
		for (ByteArrayId statsId : adapter.getSupportedStatisticsIds()) {
			@SuppressWarnings("unused")
			DataStatistics<SimpleFeature> put = stats.put(
					statsId,
					(DataStatistics<SimpleFeature>) components.getDataStore().getStatsStore().getDataStatistics(
							adapter.getAdapterId(),
							statsId,
							composeAuthorizations()));
		}
		return stats;
	}

}
