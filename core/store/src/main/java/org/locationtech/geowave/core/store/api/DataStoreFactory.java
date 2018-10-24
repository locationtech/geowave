package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;

/**
 * This is a very simple way to create a data store given an instance of that
 * particular data store's options
 */
public class DataStoreFactory
{
	/**
	 * Create a data store given that particular datastore implementation's
	 * options. The options usually define connection parameters as well as
	 * other useful configuration particular to that datastore.
	 *
	 * @param requiredOptions
	 *            the options for the desired data store
	 * @return the data store
	 */
	public static DataStore createDataStore(
			final StoreFactoryOptions requiredOptions ) {
		return new DataStorePluginOptions(
				requiredOptions).createDataStore();
	}
}
