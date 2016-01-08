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

import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStoreFactorySpi;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStoreFactorySpi;
import mil.nga.giat.geowave.core.store.config.AbstractConfigOption;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.config.StringConfigOption;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.IndexStoreFactorySpi;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStoreFactorySpi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoWaveStoreFinder
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveStoreFinder.class);
	private static String STORE_HINT_KEY = "store_name";

	public static final StringConfigOption STORE_HINT_OPTION = new StringConfigOption(
			STORE_HINT_KEY,
			"Set the GeoWave store, by default it will try to discover based on matching config options. " + getStoreNames(),
			true);
	private static Map<String, StoreFactoryFamilySpi> registeredStoreFactoryFamilies = null;
	private static Map<String, DataStoreFactorySpi> registeredDataStoreFactories = null;
	private static Map<String, AdapterStoreFactorySpi> registeredAdapterStoreFactories = null;
	private static Map<String, DataStatisticsStoreFactorySpi> registeredDataStatisticsStoreFactories = null;
	private static Map<String, IndexStoreFactorySpi> registeredIndexStoreFactories = null;
	private static Map<String, SecondaryIndexDataStoreFactorySpi> registeredSecondaryIndexDataStoreFactories = null;

	public static DataStatisticsStore createDataStatisticsStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		final DataStatisticsStoreFactorySpi factory = findDataStatisticsStoreFactory(configOptions);
		if (factory == null) {
			return null;
		}
		return factory.createStore(
				configOptions,
				namespace);
	}

	public static DataStore createDataStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		final DataStoreFactorySpi factory = findDataStoreFactory(configOptions);
		if (factory == null) {
			return null;
		}
		return factory.createStore(
				configOptions,
				namespace);
	}

	public static AdapterStore createAdapterStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		final AdapterStoreFactorySpi factory = findAdapterStoreFactory(configOptions);
		if (factory == null) {
			return null;
		}
		return factory.createStore(
				configOptions,
				namespace);
	}

	public static IndexStore createIndexStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		final IndexStoreFactorySpi factory = findIndexStoreFactory(configOptions);
		if (factory == null) {
			return null;
		}
		return factory.createStore(
				configOptions,
				namespace);
	}

	public static SecondaryIndexDataStore createSecondaryIndexDataStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		final SecondaryIndexDataStoreFactorySpi factory = findSecondaryIndexDataStoreFactory(configOptions);
		if (factory == null) {
			return null;
		}
		return factory.createStore(
				configOptions,
				namespace);
	}

	private static List<String> getMissingRequiredOptions(
			final GenericStoreFactory<?> factory,
			final Map<String, Object> configOptions ) {
		final AbstractConfigOption<?>[] options = factory.getOptions();
		final List<String> missing = new ArrayList<String>();
		for (final AbstractConfigOption<?> option : options) {
			if (!option.isOptional() && !configOptions.containsKey(option.getName())) {
				missing.add(option.getName());
			}
		}
		return missing;
	}

	public static DataStoreFactorySpi findDataStoreFactory(
			final Map<String, Object> configOptions ) {
		final Map<String, DataStoreFactorySpi> factories = getRegisteredDataStoreFactories();
		return findStore(
				factories,
				configOptions,
				"data");
	}

	public static IndexStoreFactorySpi findIndexStoreFactory(
			final Map<String, Object> configOptions ) {
		final Map<String, IndexStoreFactorySpi> factories = getRegisteredIndexStoreFactories();
		return findStore(
				factories,
				configOptions,
				"index");
	}

	public static SecondaryIndexDataStoreFactorySpi findSecondaryIndexDataStoreFactory(
			final Map<String, Object> configOptions ) {
		final Map<String, SecondaryIndexDataStoreFactorySpi> factories = getRegisteredSecondaryIndexDataStoreFactories();
		return findStore(
				factories,
				configOptions,
				"secondary index");
	}

	public static AdapterStoreFactorySpi findAdapterStoreFactory(
			final Map<String, Object> configOptions ) {
		final Map<String, AdapterStoreFactorySpi> factories = getRegisteredAdapterStoreFactories();
		return findStore(
				factories,
				configOptions,
				"adapter");
	}

	public static DataStatisticsStoreFactorySpi findDataStatisticsStoreFactory(
			final Map<String, Object> configOptions ) {
		final Map<String, DataStatisticsStoreFactorySpi> factories = getRegisteredDataStatisticsStoreFactories();
		return findStore(
				factories,
				configOptions,
				"data statistics");
	}

	private static <T extends GenericStoreFactory<?>> T findStore(
			final Map<String, T> factories,
			final Map<String, Object> configOptions,
			final String storeType ) {
		final Object storeHint = configOptions.get(STORE_HINT_KEY);
		if (storeHint != null) {
			final T factory = factories.get(storeHint.toString());
			if (factory != null) {
				final List<String> missingOptions = getMissingRequiredOptions(
						factory,
						configOptions);
				if (missingOptions.isEmpty()) {
					return factory;
				}
				LOGGER.error("Unable to find config options for " + storeType + " store '" + storeHint.toString() + "'." + ConfigUtils.getOptions(missingOptions));
				return null;
			}
			else {
				LOGGER.error("Unable to find " + storeType + " store '" + storeHint.toString() + "'");
				return null;
			}
		}
		int matchingFactoryOptionCount = -1;
		T matchingFactory = null;
		boolean matchingFactoriesHaveSameOptionCount = false;
		// if the hint is not provided, the factory finder will attempt to find
		// a factory that does not have any missing options; if multiple
		// factories will match, the one with the most options will be used with
		// the assumption that it has the most specificity and closest match of
		// the arguments; if there are multiple factories that match and have
		// the same number of options, arbitrarily the last one will be chosen
		// and a warning message will be logged
		for (final Entry<String, T> entry : factories.entrySet()) {
			final T factory = entry.getValue();
			final List<String> missingOptions = getMissingRequiredOptions(
					factory,
					configOptions);
			if (missingOptions.isEmpty() && ((matchingFactory == null) || (factory.getOptions().length >= matchingFactoryOptionCount))) {
				matchingFactory = factory;
				matchingFactoriesHaveSameOptionCount = (factory.getOptions().length == matchingFactoryOptionCount);
				matchingFactoryOptionCount = factory.getOptions().length;
			}
		}
		if (matchingFactory == null) {
			LOGGER.error("Unable to find any valid " + storeType + " store");
		}
		else if (matchingFactoriesHaveSameOptionCount) {
			LOGGER.warn("Multiple valid stores found with equal specificity for " + storeType + " store");
			LOGGER.warn(matchingFactory.getName() + " will be automatically chosen");
		}
		return matchingFactory;
	}

	private static String getStoreNames() {
		final Set<String> uniqueNames = new HashSet<String>();
		uniqueNames.addAll(getRegisteredDataStoreFactories().keySet());
		uniqueNames.addAll(getRegisteredAdapterStoreFactories().keySet());
		uniqueNames.addAll(getRegisteredIndexStoreFactories().keySet());
		uniqueNames.addAll(getRegisteredDataStatisticsStoreFactories().keySet());
		return ConfigUtils.getOptions(
				uniqueNames).toString();
	}

	public static synchronized Map<String, DataStoreFactorySpi> getRegisteredDataStoreFactories() {
		registeredDataStoreFactories = getRegisteredFactories(
				DataStoreFactorySpi.class,
				registeredDataStoreFactories);
		return registeredDataStoreFactories;
	}

	public static synchronized Map<String, StoreFactoryFamilySpi> getRegisteredStoreFactoryFamilies() {
		registeredStoreFactoryFamilies = getRegisteredFactories(
				StoreFactoryFamilySpi.class,
				registeredStoreFactoryFamilies);
		return registeredStoreFactoryFamilies;
	}

	public static synchronized Map<String, DataStatisticsStoreFactorySpi> getRegisteredDataStatisticsStoreFactories() {
		registeredDataStatisticsStoreFactories = getRegisteredFactories(
				DataStatisticsStoreFactorySpi.class,
				registeredDataStatisticsStoreFactories);
		return registeredDataStatisticsStoreFactories;
	}

	public static synchronized Map<String, AdapterStoreFactorySpi> getRegisteredAdapterStoreFactories() {
		registeredAdapterStoreFactories = getRegisteredFactories(
				AdapterStoreFactorySpi.class,
				registeredAdapterStoreFactories);
		return registeredAdapterStoreFactories;
	}

	public static synchronized Map<String, SecondaryIndexDataStoreFactorySpi> getRegisteredSecondaryIndexDataStoreFactories() {
		registeredSecondaryIndexDataStoreFactories = getRegisteredFactories(
				SecondaryIndexDataStoreFactorySpi.class,
				registeredSecondaryIndexDataStoreFactories);
		return registeredSecondaryIndexDataStoreFactories;
	}

	public static synchronized Map<String, IndexStoreFactorySpi> getRegisteredIndexStoreFactories() {
		registeredIndexStoreFactories = getRegisteredFactories(
				IndexStoreFactorySpi.class,
				registeredIndexStoreFactories);
		return registeredIndexStoreFactories;
	}

	public static synchronized AbstractConfigOption<?>[] getAllOptions(
			final StoreFactoryFamilySpi storeFactoryFamily ) {
		final List<AbstractConfigOption<?>> allOptions = new ArrayList<AbstractConfigOption<?>>();
		allOptions.addAll(Arrays.asList(storeFactoryFamily.getDataStoreFactory().getOptions()));
		allOptions.addAll(Arrays.asList(storeFactoryFamily.getIndexStoreFactory().getOptions()));
		allOptions.addAll(Arrays.asList(storeFactoryFamily.getDataStatisticsStoreFactory().getOptions()));
		allOptions.addAll(Arrays.asList(storeFactoryFamily.getAdapterStoreFactory().getOptions()));
		return allOptions.toArray(new AbstractConfigOption[] {});
	}

	public static synchronized AbstractConfigOption<?>[] getAllOptions() {
		final List<AbstractConfigOption<?>> allOptions = new ArrayList<AbstractConfigOption<?>>();
		for (final DataStoreFactorySpi f : getRegisteredDataStoreFactories().values()) {
			allOptions.addAll(Arrays.asList(f.getOptions()));
		}
		for (final IndexStoreFactorySpi f : getRegisteredIndexStoreFactories().values()) {
			allOptions.addAll(Arrays.asList(f.getOptions()));
		}
		for (final DataStatisticsStoreFactorySpi f : getRegisteredDataStatisticsStoreFactories().values()) {
			allOptions.addAll(Arrays.asList(f.getOptions()));
		}
		for (final AdapterStoreFactorySpi f : getRegisteredAdapterStoreFactories().values()) {
			allOptions.addAll(Arrays.asList(f.getOptions()));
		}
		return allOptions.toArray(new AbstractConfigOption[] {});
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
