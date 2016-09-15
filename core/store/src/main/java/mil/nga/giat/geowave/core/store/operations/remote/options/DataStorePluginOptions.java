package mil.nga.giat.geowave.core.store.operations.remote.options;

import java.util.Map;

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.api.DefaultPluginOptions;
import mil.nga.giat.geowave.core.cli.api.PluginOptions;
import mil.nga.giat.geowave.core.store.DataStore;
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

	private String storeType;

	// This is the plugin loaded from SPI based on "datastore"
	private StoreFactoryFamilySpi factoryPlugin = null;

	// These are the options loaded from factoryPlugin based on "datastore"
	@ParametersDelegate
	private StoreFactoryOptions factoryOptions = null;

	public DataStorePluginOptions() {}

	/**
	 * From the given options (like 'username', 'password') and the given
	 * storeType (like 'accumulo'), setup this plugin options to be able to
	 * create data stores.
	 *
	 * @param storeType
	 * @param options
	 */
	public DataStorePluginOptions(
			final String storeType,
			final Map<String, String> options ) {
		selectPlugin(storeType);
		ConfigUtils.populateOptionsFromList(
				getFactoryOptions(),
				options);
	}

	public DataStorePluginOptions(
			final String storeType,
			final StoreFactoryFamilySpi factoryPlugin,
			final StoreFactoryOptions factoryOptions ) {
		this.storeType = storeType;
		this.factoryPlugin = factoryPlugin;
		this.factoryOptions = factoryOptions;
	}

	/**
	 * This method will allow the user to specify the desired factory, such as
	 * 'accumulo' or 'hbase'.
	 */
	@Override
	public void selectPlugin(
			final String qualifier ) {
		storeType = qualifier;
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

	public Map<String, String> getFactoryOptionsAsMap() {
		return ConfigUtils.populateListFromOptions(factoryOptions);
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

	@Override
	public String getType() {
		return storeType;
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
