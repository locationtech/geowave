package mil.nga.giat.geowave.test;

import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

public class TestDataStoreOptions extends
		DataStorePluginOptions
{
	private final GeoWaveStoreType storeType;

	public TestDataStoreOptions(
			final GeoWaveStoreType storeType ) {
		super();
		this.storeType = storeType;
	}

	public GeoWaveStoreType getStoreType() {
		return storeType;
	}
}
