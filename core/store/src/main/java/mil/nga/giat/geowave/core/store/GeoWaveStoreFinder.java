package mil.nga.giat.geowave.core.store;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;

import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.config.ConfigOption;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoWaveStoreFinder
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveStoreFinder.class);
	private static String STORE_HINT_KEY = "store_name";

	public static final ConfigOption STORE_HINT_OPTION = new ConfigOption(
			STORE_HINT_KEY,
			"Set the GeoWave store, by default it will try to discover based on matching config options. "
					+ getStoreNames(),
			true,
			String.class);

	private static Map<String, StoreFactoryFamilySpi> registeredStoreFactoryFamilies = null;

	public static DataStatisticsStore createDataStatisticsStore(
			final Map<String, String> configOptions ) {
		final StoreFactoryFamilySpi factory = findStoreFamily(configOptions);
		if (factory == null) {
			return null;
		}
		return factory.getDataStatisticsStoreFactory().createStore(
				ConfigUtils.populateOptionsFromList(
						factory.getDataStatisticsStoreFactory().createOptionsInstance(),
						configOptions));
	}

	public static DataStore createDataStore(
			final Map<String, String> configOptions ) {
		final StoreFactoryFamilySpi factory = findStoreFamily(configOptions);
		if (factory == null) {
			return null;
		}
		return factory.getDataStoreFactory().createStore(
				ConfigUtils.populateOptionsFromList(
						factory.getDataStoreFactory().createOptionsInstance(),
						configOptions));
	}

	public static AdapterStore createAdapterStore(
			final Map<String, String> configOptions ) {
		final StoreFactoryFamilySpi factory = findStoreFamily(configOptions);
		if (factory == null) {
			return null;
		}
		return factory.getAdapterStoreFactory().createStore(
				ConfigUtils.populateOptionsFromList(
						factory.getAdapterStoreFactory().createOptionsInstance(),
						configOptions));
	}

	public static AdapterIndexMappingStore createAdapterIndexMappingStore(
			final Map<String, String> configOptions ) {
		final StoreFactoryFamilySpi factory = findStoreFamily(configOptions);
		if (factory == null) {
			return null;
		}
		return factory.getAdapterIndexMappingStoreFactory().createStore(
				ConfigUtils.populateOptionsFromList(
						factory.getAdapterIndexMappingStoreFactory().createOptionsInstance(),
						configOptions));
	}

	public static IndexStore createIndexStore(
			final Map<String, String> configOptions ) {
		final StoreFactoryFamilySpi factory = findStoreFamily(configOptions);
		if (factory == null) {
			return null;
		}
		return factory.getIndexStoreFactory().createStore(
				ConfigUtils.populateOptionsFromList(
						factory.getIndexStoreFactory().createOptionsInstance(),
						configOptions));
	}

	public static SecondaryIndexDataStore createSecondaryIndexDataStore(
			final Map<String, String> configOptions ) {
		final StoreFactoryFamilySpi factory = findStoreFamily(configOptions);
		if (factory == null) {
			return null;
		}
		return factory.getSecondaryIndexDataStore().createStore(
				ConfigUtils.populateOptionsFromList(
						factory.getSecondaryIndexDataStore().createOptionsInstance(),
						configOptions));
	}

	private static List<String> getMissingRequiredOptions(
			final StoreFactoryFamilySpi factory,
			final Map<String, String> configOptions ) {
		final ConfigOption[] options = ConfigUtils.createConfigOptionsFromJCommander(factory
				.getDataStoreFactory()
				.createOptionsInstance());
		final List<String> missing = new ArrayList<String>();
		for (final ConfigOption option : options) {
			if (!option.isOptional() && !configOptions.containsKey(option.getName())) {
				missing.add(option.getName());
			}
		}
		return missing;
	}

	public static StoreFactoryFamilySpi findStoreFamily(
			final Map<String, String> configOptions ) {
		final Object storeHint = configOptions.get(STORE_HINT_KEY);
		Map<String, StoreFactoryFamilySpi> internalStoreFamilies = getRegisteredStoreFactoryFamilies();
		if (storeHint != null) {
			final StoreFactoryFamilySpi factory = internalStoreFamilies.get(storeHint.toString());
			if (factory != null) {
				final List<String> missingOptions = getMissingRequiredOptions(
						factory,
						configOptions);
				if (missingOptions.isEmpty()) {
					return factory;
				}
				LOGGER.error("Unable to find config options for store '" + storeHint.toString() + "'."
						+ ConfigUtils.getOptions(missingOptions));
				return null;
			}
			else {
				LOGGER.error("Unable to find store '" + storeHint.toString() + "'");
				return null;
			}
		}
		int matchingFactoryOptionCount = -1;
		StoreFactoryFamilySpi matchingFactory = null;
		boolean matchingFactoriesHaveSameOptionCount = false;
		// if the hint is not provided, the factory finder will attempt to find
		// a factory that does not have any missing options; if multiple
		// factories will match, the one with the most options will be used with
		// the assumption that it has the most specificity and closest match of
		// the arguments; if there are multiple factories that match and have
		// the same number of options, arbitrarily the last one will be chosen
		// and a warning message will be logged
		LOGGER.debug("Finding Factories (size): " + internalStoreFamilies.size());

		for (final Entry<String, StoreFactoryFamilySpi> entry : internalStoreFamilies.entrySet()) {
			final StoreFactoryFamilySpi factory = entry.getValue();
			final List<String> missingOptions = getMissingRequiredOptions(
					factory,
					configOptions);
			ConfigOption[] factoryOptions = ConfigUtils.createConfigOptionsFromJCommander(factory
					.getDataStoreFactory()
					.createOptionsInstance());
			LOGGER.debug("OPTIONS -- length: " + factoryOptions.length + ", " + factory.getName());
			if (missingOptions.isEmpty()
					&& ((matchingFactory == null) || (factoryOptions.length >= matchingFactoryOptionCount))) {
				matchingFactory = factory;
				matchingFactoriesHaveSameOptionCount = (factoryOptions.length == matchingFactoryOptionCount);
				matchingFactoryOptionCount = factoryOptions.length;
			}
		}

		if (matchingFactory == null) {
			LOGGER.error("Unable to find any valid store");
		}
		else if (matchingFactoriesHaveSameOptionCount) {
			LOGGER.warn("Multiple valid stores found with equal specificity for store");
			LOGGER.warn(matchingFactory.getName() + " will be automatically chosen");
		}
		return matchingFactory;
	}

	private static String getStoreNames() {
		final Set<String> uniqueNames = new HashSet<String>();
		uniqueNames.addAll(getRegisteredStoreFactoryFamilies().keySet());
		return ConfigUtils.getOptions(
				uniqueNames).toString();
	}

	public static synchronized Map<String, StoreFactoryFamilySpi> getRegisteredStoreFactoryFamilies() {
		registeredStoreFactoryFamilies = getRegisteredFactories(
				StoreFactoryFamilySpi.class,
				registeredStoreFactoryFamilies);
		return registeredStoreFactoryFamilies;
	}

	public static synchronized ConfigOption[] getAllOptions(
			final StoreFactoryFamilySpi storeFactoryFamily ) {
		final List<ConfigOption> allOptions = new ArrayList<ConfigOption>();
		allOptions.addAll(Arrays.asList(ConfigUtils.createConfigOptionsFromJCommander(storeFactoryFamily
				.getDataStoreFactory()
				.createOptionsInstance())));
		return allOptions.toArray(new ConfigOption[] {});
	}

	public static synchronized ConfigOption[] getAllOptions() {
		final List<ConfigOption> allOptions = new ArrayList<ConfigOption>();
		for (final StoreFactoryFamilySpi f : getRegisteredStoreFactoryFamilies().values()) {
			ConfigOption[] factoryOptions = ConfigUtils.createConfigOptionsFromJCommander(f
					.getDataStoreFactory()
					.createOptionsInstance());
			allOptions.addAll(Arrays.asList(factoryOptions));
		}
		return allOptions.toArray(new ConfigOption[] {});
	}

	public static synchronized ConfigOption[] getRequiredOptions(
			final StoreFactoryFamilySpi storeFactoryFamily ) {
		final List<ConfigOption> requiredOptions = new ArrayList<ConfigOption>();
		for (final ConfigOption option : getAllOptions(storeFactoryFamily)) {
			if (!option.isOptional()) {
				requiredOptions.add(option);
			}
		}
		return requiredOptions.toArray(new ConfigOption[] {});
	}

	private static <T extends GenericFactory> Map<String, T> getRegisteredFactories(
			final Class<T> cls,
			Map<String, T> registeredFactories ) {
		if (registeredFactories == null) {
			registeredFactories = new HashMap<String, T>();
			final Iterator<T> storeFactories = ServiceLoader.load(
					cls).iterator();
			while (storeFactories.hasNext()) {
				final T storeFactory = storeFactories.next();
				if (storeFactory != null) {
					final String name = storeFactory.getName();
					registeredFactories.put(
							ConfigUtils.cleanOptionName(name),
							storeFactory);
				}
			}
		}
		return registeredFactories;
	}
}
