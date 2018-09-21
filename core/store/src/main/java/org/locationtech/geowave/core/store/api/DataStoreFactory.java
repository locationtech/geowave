package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;

public class DataStoreFactory
{
	public static DataStore createDataStore(
			StoreFactoryOptions requiredOptions ) {
		return new DataStorePluginOptions(
				requiredOptions).createDataStore();
	}
}
