package mil.nga.giat.geowave.test;

import mil.nga.giat.geowave.core.store.plugins.DataStorePluginOptions;

public interface StoreTestEnvironment extends
		TestEnvironment
{
	public DataStorePluginOptions getDataStoreOptions(
			String namespace );
}
