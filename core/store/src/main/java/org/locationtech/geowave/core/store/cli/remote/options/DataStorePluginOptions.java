/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.cli.remote.options;

import java.util.Map;

import org.locationtech.geowave.core.cli.api.DefaultPluginOptions;
import org.locationtech.geowave.core.cli.api.PluginOptions;
import org.locationtech.geowave.core.store.DataStore;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.AdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.config.ConfigUtils;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.SecondaryIndexDataStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;

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

	public void setFactoryFamily(
			StoreFactoryFamilySpi factoryPlugin ) {
		this.factoryPlugin = factoryPlugin;
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

	public PersistentAdapterStore createAdapterStore() {
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

	public InternalAdapterStore createInternalAdapterStore() {
		return getFactoryFamily().getInternalAdapterStoreFactory().createStore(
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
