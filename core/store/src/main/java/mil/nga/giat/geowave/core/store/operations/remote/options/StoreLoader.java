package mil.nga.giat.geowave.core.store.operations.remote.options;

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;
import mil.nga.giat.geowave.core.cli.utils.JCommanderParameterUtils;
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
	private final static Logger LOGGER = LoggerFactory.getLogger(StoreLoader.class);

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

		// load all plugin options and initialize dataStorePlugin with type and
		// options
		if (!dataStorePlugin.load(
				props,
				namespace)) {
			return false;
		}

		// knowing the datastore plugin options and class type, get all fields
		// and parameters in order to detect which are password fields
		if (dataStorePlugin.getFactoryOptions() != null) {
			File tokenFile = SecurityUtils.getFormattedTokenKeyFileForConfig(configFile);
			Field[] fields = dataStorePlugin.getFactoryOptions().getClass().getDeclaredFields();
			for (Field field : fields) {
				for (Annotation annotation : field.getAnnotations()) {
					if (annotation.annotationType() == Parameter.class) {
						Parameter parameter = (Parameter) annotation;
						if (JCommanderParameterUtils.isPassword(parameter)) {
							String storeFieldName = (namespace != null && !"".equals(namespace.trim())) ? namespace
									+ "." + DataStorePluginOptions.OPTS + "." + field.getName() : field.getName();
							if (props.containsKey(storeFieldName)) {
								String value = props.getProperty(storeFieldName);
								String decryptedValue = value;
								try {
									decryptedValue = SecurityUtils.decryptHexEncodedValue(
											value,
											tokenFile.getAbsolutePath());
								}
								catch (Exception e) {
									LOGGER.error(
											"An error occurred encrypting specified password value: "
													+ e.getLocalizedMessage(),
											e);
								}
								props.setProperty(
										storeFieldName,
										decryptedValue);
							}
						}
					}
				}
			}
			tokenFile = null;
		}
		// reload datastore plugin with new password-encrypted properties
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
