package mil.nga.giat.geowave.core.store.cli.remote.options;

import java.io.File;
import java.util.Properties;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

/**
 * This is a convenience class which sets up some obvious values in the
 * OperationParams based on the parsed 'store name' from the main parameter. The
 * other parameters are saved in case they need to be used.
 */
public class StoreLoader
{

	private final String storeName;

	private DataStorePluginOptions dataStorePlugin = null;

	/**
	 * Constructor
	 */
	public StoreLoader(
			final String store ) {
		this.storeName = store;
	}

	/**
	 * Attempt to load the datastore configuration from the config file.
	 * 
	 * @param configFile
	 * @return
	 */
	public boolean loadFromConfig(
			File configFile ) {

		String namespace = DataStorePluginOptions.getStoreNamespace(storeName);
		Properties props = ConfigOptions.loadProperties(
				configFile,
				"^" + namespace);

		dataStorePlugin = new DataStorePluginOptions();

		if (!dataStorePlugin.load(
				props,
				namespace)) {
			return false;
		}

		return true;
	}

	public DataStorePluginOptions getDataStorePlugin() {
		return dataStorePlugin;
	}

	public void setDataStorePlugin(
			DataStorePluginOptions dataStorePlugin ) {
		this.dataStorePlugin = dataStorePlugin;
	}

	public String getStoreName() {
		return storeName;
	}

	public StoreFactoryFamilySpi getFactoryFamily() {
		return dataStorePlugin.getFactoryFamily();
	}

	public StoreFactoryOptions getFactoryOptions() {
		return dataStorePlugin.getFactoryOptions();
	}

	public DataStore createDataStore() {
		return dataStorePlugin.createDataStore();
	}

	public AdapterStore createAdapterStore() {
		return dataStorePlugin.createAdapterStore();
	}

	public IndexStore createIndexStore() {
		return dataStorePlugin.createIndexStore();
	}

	public DataStatisticsStore createDataStatisticsStore() {
		return dataStorePlugin.createDataStatisticsStore();
	}

	public SecondaryIndexDataStore createSecondaryIndexStore() {
		return dataStorePlugin.createSecondaryIndexStore();
	}

	public AdapterIndexMappingStore createAdapterIndexMappingStore() {
		return dataStorePlugin.createAdapterIndexMappingStore();
	}
}
