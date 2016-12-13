package mil.nga.giat.geowave.test;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

public abstract class StoreTestEnvironment implements
		TestEnvironment
{
	protected abstract GenericStoreFactory<DataStore> getDataStoreFactory();

	protected abstract GeoWaveStoreType getStoreType();

	protected abstract void initOptions(
			StoreFactoryOptions options );

	public DataStorePluginOptions getDataStoreOptions(
			final GeoWaveTestStore store ) {
		final DataStorePluginOptions pluginOptions = new TestDataStoreOptions(
				getStoreType());
		final GenericStoreFactory<DataStore> factory = getDataStoreFactory();
		StoreFactoryOptions opts = factory.createOptionsInstance();
		initOptions(opts);
		opts.setGeowaveNamespace(store.namespace());
		final Map<String, String> optionOverrides = new HashMap<>();
		// now allow for overrides to take precedence
		for (final String optionOverride : store.options()) {
			if (optionOverride.contains("=")) {
				final String[] kv = optionOverride.split("=");
				optionOverrides.put(
						kv[0],
						kv[1]);
			}
		}
		if (!optionOverrides.isEmpty()) {
			opts = ConfigUtils.populateOptionsFromList(
					opts,
					optionOverrides);
		}

		pluginOptions.selectPlugin(factory.getType());
		pluginOptions.setFactoryOptions(opts);
		return pluginOptions;
	}
}
