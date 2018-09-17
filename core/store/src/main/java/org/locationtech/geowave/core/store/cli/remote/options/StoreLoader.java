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

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Properties;

import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.operations.config.security.utils.SecurityUtils;
import org.locationtech.geowave.core.cli.utils.JCommanderParameterUtils;
import org.locationtech.geowave.core.store.DataStore;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.AdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.SecondaryIndexDataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

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

		return loadFromConfig(
				ConfigOptions.loadProperties(
						configFile,
						"^" + namespace),
				namespace,
				configFile);
	}

	/**
	 * Attempt to load the datastore configuration from the config file.
	 * 
	 * @param configFile
	 * @return
	 */
	public boolean loadFromConfig(
			Properties props,
			String namespace,
			File configFile ) {

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
		if (configFile != null && dataStorePlugin.getFactoryOptions() != null) {
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

	public PersistentAdapterStore createAdapterStore() {
		return dataStorePlugin.createAdapterStore();
	}

	public InternalAdapterStore createInternalAdapterStore() {
		return dataStorePlugin.createInternalAdapterStore();
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
