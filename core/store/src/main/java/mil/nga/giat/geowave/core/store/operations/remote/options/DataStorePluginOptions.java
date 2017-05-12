package mil.nga.giat.geowave.core.store.operations.remote.options;

import java.util.Map;

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.api.DefaultPluginOptions;
import mil.nga.giat.geowave.core.cli.api.PluginOptions;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

/**
 * Class is used to facilitate loading of a DataStore from options specified on
 * the command line.
 */
public class DataStorePluginOptions extends
		DefaultPluginOptions implements
		PluginOptions
{

	public static final String DATASTORE_PROPERTY_NAMESPACE = "store";
	public static final String DEFAULT_PROPERTY_NAMESPACE = "storedefault";

	// This is the plugin loaded from SPI based on "datastore"
	private StoreFactoryFamilySpi factoryPlugin = null;

	// These are the options loaded from factoryPlugin based on "datastore"
	@ParametersDelegate
	private StoreFactoryOptions factoryOptions = null;

	public DataStorePluginOptions() {}

	/**
	 * From the given options (like 'username', 'password') setup this plugin
	 * options to be able to create data stores.
	 *
	 * @param options
	 */
	public DataStorePluginOptions(
			final Map<String, String> options )
			throws IllegalArgumentException {
		factoryPlugin = GeoWaveStoreFinder.findStoreFamily(options);
		if (factoryPlugin == null) {
			throw new IllegalArgumentException(
					"Cannot find store plugin factory");
		}
		factoryOptions = factoryPlugin.getDataStoreFactory().createOptionsInstance();
		ConfigUtils.populateOptionsFromList(
				getFactoryOptions(),
				options);
	}

	public DataStorePluginOptions(
			final StoreFactoryOptions factoryOptions ) {
		this.factoryOptions = factoryOptions;
		factoryPlugin = factoryOptions.getStoreFactory();
	}

	/**
	 * This method will allow the user to specify the desired factory, such as
	 * 'accumulo' or 'hbase'.
	 */
	@Override
	public void selectPlugin(
			final String qualifier ) {
		if (qualifier != null) {
			final Map<String, StoreFactoryFamilySpi> factories = GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies();
			factoryPlugin = factories.get(qualifier);
			if (factoryPlugin == null) {
				throw new ParameterException(
						"Unknown datastore type: " + qualifier);
			}
			factoryOptions = factoryPlugin.getDataStoreFactory().createOptionsInstance();
		}
		else {
			factoryPlugin = null;
			factoryOptions = null;
		}
	}

	public Map<String, String> getOptionsAsMap() {
		final Map<String, String> configOptions = ConfigUtils.populateListFromOptions(factoryOptions);
		if (factoryPlugin != null) {
			configOptions.put(
					GeoWaveStoreFinder.STORE_HINT_OPTION.getName(),
					factoryPlugin.getType());
		}
		return configOptions;
	}

	public void setFactoryOptions(
			final StoreFactoryOptions factoryOptions ) {
		this.factoryOptions = factoryOptions;
	}

	public StoreFactoryFamilySpi getFactoryFamily() {
		return factoryPlugin;
	}

	public StoreFactoryOptions getFactoryOptions() {
		return factoryOptions;
	}

	public DataStore createDataStore() {
		return getFactoryFamily().getDataStoreFactory().createStore(
				getFactoryOptions());
	}

	public AdapterStore createAdapterStore() {
		return getFactoryFamily().getAdapterStoreFactory().createStore(
				getFactoryOptions());
	}

	public IndexStore createIndexStore() {
		return getFactoryFamily().getIndexStoreFactory().createStore(
				getFactoryOptions());
	}

	public DataStatisticsStore createDataStatisticsStore() {
		return getFactoryFamily().getDataStatisticsStoreFactory().createStore(
				getFactoryOptions());
	}

	public SecondaryIndexDataStore createSecondaryIndexStore() {
		return getFactoryFamily().getSecondaryIndexDataStore().createStore(
				getFactoryOptions());
	}

	public AdapterIndexMappingStore createAdapterIndexMappingStore() {
		return getFactoryFamily().getAdapterIndexMappingStoreFactory().createStore(
				getFactoryOptions());
	}

	public DataStoreOperations createDataStoreOperations() {
		return getFactoryFamily().getDataStoreOperationsFactory().createStore(
				getFactoryOptions());
	}

	@Override
	public String getType() {
		if (factoryPlugin == null) {
			return null;
		}
		return factoryPlugin.getType();
	}

	public static String getStoreNamespace(
			final String name ) {
		return String.format(
				"%s.%s",
				DATASTORE_PROPERTY_NAMESPACE,
				name);
	}

	public String getGeowaveNamespace() {
		return getFactoryOptions().getGeowaveNamespace();
	}

}
