/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.SPIServiceRegistry;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.config.ConfigOption;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

public class GeoWaveStoreFinder
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveStoreFinder.class);
	public static String STORE_HINT_KEY = "store_name";

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
		final ConfigOption[] options = ConfigUtils.createConfigOptionsFromJCommander(
				factory.getDataStoreFactory().createOptionsInstance(),
				false);
		final List<String> missing = new ArrayList<String>();
		for (final ConfigOption option : options) {
			if (!option.isOptional()
					&& (!configOptions.containsKey(option.getName()) || (configOptions.get(option.getName())
							.equals("null")))) {
				missing.add(option.getName());
			}
		}
		return missing;
	}

	private static List<String> getMatchingRequiredOptions(
			final StoreFactoryFamilySpi factory,
			final Map<String, String> configOptions ) {
		final ConfigOption[] options = ConfigUtils.createConfigOptionsFromJCommander(
				factory.getDataStoreFactory().createOptionsInstance(),
				false);
		final List<String> matching = new ArrayList<String>();
		for (final ConfigOption option : options) {
			if (!option.isOptional() && (configOptions.containsKey(option.getName()) && (!configOptions.get(
					option.getName()).equals(
					"null")))) {
				matching.add(option.getName());
			}
		}
		return matching;
	}

	public static StoreFactoryFamilySpi findStoreFamily(
			final Map<String, String> configOptions ) {
		final Object storeHint = configOptions.get(STORE_HINT_KEY);
		final Map<String, StoreFactoryFamilySpi> internalStoreFamilies = getRegisteredStoreFactoryFamilies();
		if (storeHint != null) {
			final StoreFactoryFamilySpi factory = internalStoreFamilies.get(storeHint.toString());
			if (factory != null) {
				final List<String> missingOptions = getMissingRequiredOptions(
						factory,
						configOptions);
				if (missingOptions.isEmpty()) {
					return factory;
				}
				// HP Fortify "Improper Output Neutralization" false positive
				// What Fortify considers "user input" comes only
				// from users with OS-level access anyway
				LOGGER.error("Unable to find config options for store '" + storeHint.toString() + "'."
						+ ConfigUtils.getOptions(missingOptions));
				return null;
			}
			else {
				// HP Fortify "Improper Output Neutralization" false positive
				// What Fortify considers "user input" comes only
				// from users with OS-level access anyway
				LOGGER.error("Unable to find store '" + storeHint.toString() + "'");
				return null;
			}
		}

		// if the hint is not provided, the factory finder will attempt to find
		// a factory that has an exact match meaning that all required params
		// are provided and all provided params are defined as at least optional
		// params

		for (final Entry<String, StoreFactoryFamilySpi> entry : internalStoreFamilies.entrySet()) {
			if (exactMatch(
					entry.getValue(),
					configOptions)) {
				return entry.getValue();
			}
		}
		// if it cannot find and exact match it will attempt to does not have
		// any missing options; if multiple/ factories will match, the one with
		// the most required matching options will be used with
		// the assumption that it has the most specificity and closest match of
		// the arguments; if there are multiple factories that match and have
		// the same number of required matching options, arbitrarily the last
		// one will be chosen
		// and a warning message will be logged
		int matchingFactoryRequiredOptionsCount = -1;
		StoreFactoryFamilySpi matchingFactory = null;
		boolean matchingFactoriesHaveSameRequiredOptionsCount = false;
		LOGGER.debug("Finding Factories (size): " + internalStoreFamilies.size());

		for (final Entry<String, StoreFactoryFamilySpi> entry : internalStoreFamilies.entrySet()) {
			final StoreFactoryFamilySpi factory = entry.getValue();
			final List<String> missingOptions = getMissingRequiredOptions(
					factory,
					configOptions);
			final List<String> matchingOptions = getMatchingRequiredOptions(
					factory,
					configOptions);
			if (missingOptions.isEmpty()
					&& ((matchingFactory == null) || (matchingOptions.size() >= matchingFactoryRequiredOptionsCount))) {
				matchingFactory = factory;
				matchingFactoriesHaveSameRequiredOptionsCount = (matchingOptions.size() == matchingFactoryRequiredOptionsCount);
				matchingFactoryRequiredOptionsCount = matchingOptions.size();
			}
		}

		if (matchingFactory == null) {
			LOGGER.error("Unable to find any valid store");
		}
		else if (matchingFactoriesHaveSameRequiredOptionsCount) {
			LOGGER.warn("Multiple valid stores found with equal specificity for store");
			LOGGER.warn(matchingFactory.getType() + " will be automatically chosen");
		}
		return matchingFactory;
	}

	private static String getStoreNames() {
		final Set<String> uniqueNames = new HashSet<String>();
		uniqueNames.addAll(getRegisteredStoreFactoryFamilies().keySet());
		return ConfigUtils.getOptions(
				uniqueNames).toString();
	}

	public static boolean exactMatch(
			final StoreFactoryFamilySpi geowaveStoreFactoryFamily,
			final Map<String, String> params ) {
		final ConfigOption[] requiredOptions = GeoWaveStoreFinder.getRequiredOptions(geowaveStoreFactoryFamily);
		// first ensure all required options are fulfilled
		for (final ConfigOption requiredOption : requiredOptions) {
			if (!params.containsKey(requiredOption.getName())) {
				return false;
			}
		}
		// next ensure that all params match an available option
		final Set<String> availableOptions = new HashSet<String>();
		for (final ConfigOption option : GeoWaveStoreFinder.getAllOptions(
				geowaveStoreFactoryFamily,
				true)) {
			availableOptions.add(option.getName());
		}
		for (final String optionName : params.keySet()) {
			if (!availableOptions.contains(optionName) && !STORE_HINT_KEY.equals(optionName)) {
				return false;
			}
		}

		// lastly try to create the index store (pick a minimally required
		// store)
		try {
			final StoreFactoryOptions options = ConfigUtils.populateOptionsFromList(
					geowaveStoreFactoryFamily.getDataStoreFactory().createOptionsInstance(),
					params);
			geowaveStoreFactoryFamily.getIndexStoreFactory().createStore(
					options);
		}
		catch (final Exception e) {
			LOGGER.info(
					"supplied map is not able to construct index store",
					e);
			return false;
		}
		return true;
	}

	public static synchronized Map<String, StoreFactoryFamilySpi> getRegisteredStoreFactoryFamilies() {
		registeredStoreFactoryFamilies = getRegisteredFactories(
				StoreFactoryFamilySpi.class,
				registeredStoreFactoryFamilies);
		return registeredStoreFactoryFamilies;
	}

	public static synchronized ConfigOption[] getAllOptions(
			final StoreFactoryFamilySpi storeFactoryFamily,
			boolean includeHidden ) {
		final List<ConfigOption> allOptions = new ArrayList<ConfigOption>();
		allOptions.addAll(Arrays.asList(ConfigUtils.createConfigOptionsFromJCommander(
				storeFactoryFamily.getDataStoreFactory().createOptionsInstance(),
				includeHidden)));
		// TODO our JCommanderPrefixTranslator's use of reflection does not
		// follow inheritance, these are commonly inherited classes and options
		// for all data stores provided as a stop gap until we can investigate
		// allOptions.addAll(
		// Arrays.asList(
		// ConfigUtils.createConfigOptionsFromJCommander(
		// new BaseDataStoreOptions())));
		// allOptions.add(
		// new ConfigOption(
		// StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION,
		// StoreFactoryOptions.GEOWAVE_NAMESPACE_DESCRIPTION,
		// true,
		// String.class));
		return allOptions.toArray(new ConfigOption[] {});
	}

	public static synchronized ConfigOption[] getRequiredOptions(
			final StoreFactoryFamilySpi storeFactoryFamily ) {
		final List<ConfigOption> requiredOptions = new ArrayList<ConfigOption>();
		for (final ConfigOption option : getAllOptions(
				storeFactoryFamily,
				false)) {
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
			final Iterator<T> storeFactories = new SPIServiceRegistry(
					GeoWaveStoreFinder.class).load(cls);
			while (storeFactories.hasNext()) {
				final T storeFactory = storeFactories.next();
				if (storeFactory != null) {
					final String name = storeFactory.getType();
					registeredFactories.put(
							ConfigUtils.cleanOptionName(name),
							storeFactory);
				}
			}
		}
		return registeredFactories;
	}
}
