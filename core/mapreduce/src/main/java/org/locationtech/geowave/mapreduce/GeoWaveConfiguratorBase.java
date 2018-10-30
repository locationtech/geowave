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
package org.locationtech.geowave.mapreduce;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class forms the basis for GeoWave input and output format configuration.
 */
public class GeoWaveConfiguratorBase
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveConfiguratorBase.class);
	private static final String KEY_SEPARATOR = "-";

	public static enum GeoWaveConfg {
		INDEX,
		DATA_ADAPTER,
		INTERNAL_ADAPTER,
		ADAPTER_TO_INDEX,
		STORE_CONFIG_OPTION
	}

	/**
	 * Provides a configuration key for a given feature enum, prefixed by the
	 * implementingClass, and suffixed by a custom String
	 *
	 * @param implementingClass
	 *            the class whose name will be used as a prefix for the property
	 *            configuration key
	 * @param e
	 *            the enum used to provide the unique part of the configuration
	 *            key
	 *
	 * @param suffix
	 *            the custom suffix to be used in the configuration key
	 * @return the configuration key
	 */
	public static String enumToConfKey(
			final Class<?> implementingClass,
			final Enum<?> e,
			final String suffix ) {
		return enumToConfKey(
				implementingClass,
				e) + KEY_SEPARATOR + suffix;
	}

	/**
	 * Provides a configuration key for a given feature enum, prefixed by the
	 * implementingClass
	 *
	 * @param implementingClass
	 *            the class whose name will be used as a prefix for the property
	 *            configuration key
	 * @param e
	 *            the enum used to provide the unique part of the configuration
	 *            key
	 * @return the configuration key
	 */
	public static String enumToConfKey(
			final Class<?> implementingClass,
			final Enum<?> e ) {
		final String s = implementingClass.getSimpleName() + "." + e.getDeclaringClass().getSimpleName() + "."
				+ org.apache.hadoop.util.StringUtils.camelize(e.name().toLowerCase(
						Locale.ENGLISH));
		return s;
	}

	public static final <T> T getInstance(
			final Class<?> implementingClass,
			final Enum<?> e,
			final JobContext context,
			final Class<T> interfaceClass )
			throws InstantiationException,
			IllegalAccessException {
		return (T) getConfiguration(
				context).getClass(
				enumToConfKey(
						implementingClass,
						e),
				interfaceClass).newInstance();
	}

	public static final <T> T getInstance(
			final Class<?> implementingClass,
			final Enum<?> e,
			final JobContext context,
			final Class<T> interfaceClass,
			final Class<? extends T> defaultClass )
			throws InstantiationException,
			IllegalAccessException {
		return getConfiguration(
				context).getClass(
				enumToConfKey(
						implementingClass,
						e),
				defaultClass,
				interfaceClass).newInstance();
	}

	public static DataStore getDataStore(
			final Class<?> implementingClass,
			final JobContext context ) {
		return GeoWaveStoreFinder.createDataStore(getStoreOptionsMap(
				implementingClass,
				context));
	}

	public static DataStatisticsStore getDataStatisticsStore(
			final Class<?> implementingClass,
			final JobContext context ) {
		return GeoWaveStoreFinder.createDataStatisticsStore(getStoreOptionsMap(
				implementingClass,
				context));
	}

	public static void setStoreOptionsMap(
			final Class<?> implementingClass,
			final Configuration config,
			final Map<String, String> dataStoreOptions ) {
		if ((dataStoreOptions != null) && !dataStoreOptions.isEmpty()) {
			for (final Entry<String, String> entry : dataStoreOptions.entrySet()) {
				config.set(
						enumToConfKey(
								implementingClass,
								GeoWaveConfg.STORE_CONFIG_OPTION,
								entry.getKey()),
						entry.getValue());
			}
		}
		else {
			final Map<String, String> existingVals = config.getValByRegex(enumToConfKey(
					implementingClass,
					GeoWaveConfg.STORE_CONFIG_OPTION) + "*");
			for (final String k : existingVals.keySet()) {
				config.unset(k);
			}
		}
	}

	public static DataStorePluginOptions getStoreOptions(
			final Class<?> implementingClass,
			final JobContext context ) {
		final Map<String, String> options = getStoreOptionsMapInternal(
				implementingClass,
				getConfiguration(context));
		try {
			return new DataStorePluginOptions(
					options);
		}
		catch (final IllegalArgumentException e) {
			LOGGER.warn(
					"Unable to get data store options from job context",
					e);
			return null;
		}
	}

	public static Map<String, String> getStoreOptionsMap(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getStoreOptionsMapInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void addIndex(
			final Class<?> implementingClass,
			final Configuration config,
			final Index index ) {
		if (index != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							GeoWaveConfg.INDEX,
							index.getName()),
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(index)));
		}
	}

	public static Index getIndex(
			final Class<?> implementingClass,
			final JobContext context,
			final String indexName ) {
		return getIndexInternal(
				implementingClass,
				getConfiguration(context),
				indexName);
	}

	public static Short getAdapterId(
			final Class<?> implementingClass,
			final JobContext context,
			final String typeName ) {
		return getAdapterIdInternal(
				implementingClass,
				getConfiguration(context),
				typeName);
	}

	private static Short getAdapterIdInternal(
			final Class<?> implementingClass,
			final Configuration configuration,
			final String typeName ) {
		final String input = configuration.get(enumToConfKey(
				implementingClass,
				GeoWaveConfg.INTERNAL_ADAPTER,
				typeName));
		if (input != null) {
			return Short.valueOf(input);
		}
		return null;
	}

	public static String getTypeName(
			final Class<?> implementingClass,
			final JobContext context,
			final short internalAdapterId ) {
		return getTypeNameInternal(
				implementingClass,
				getConfiguration(context),
				internalAdapterId);
	}

	private static String getTypeNameInternal(
			final Class<?> implementingClass,
			final Configuration configuration,
			final short internalAdapterId ) {
		final String prefix = enumToConfKey(
				implementingClass,
				GeoWaveConfg.INTERNAL_ADAPTER);
		final Map<String, String> input = configuration.getValByRegex(prefix + "*");
		final String internalAdapterIdStr = Short.toString(internalAdapterId);
		for (final Entry<String, String> e : input.entrySet()) {
			if (e.getValue().equals(
					internalAdapterIdStr)) {
				return e.getKey().substring(
						prefix.length() + 1);
			}
		}
		return null;
	}

	public static void addTypeName(
			final Class<?> implementingClass,
			final Configuration conf,
			final String typeName,
			final short internalAdapterId ) {
		conf.set(
				enumToConfKey(
						implementingClass,
						GeoWaveConfg.INTERNAL_ADAPTER,
						typeName),
				Short.toString(internalAdapterId));
	}

	public static void addAdapterToIndexMapping(
			final Class<?> implementingClass,
			final Configuration conf,
			final AdapterToIndexMapping adapterToIndexMapping ) {
		if (adapterToIndexMapping != null) {
			conf.set(
					enumToConfKey(
							implementingClass,
							GeoWaveConfg.ADAPTER_TO_INDEX,
							Short.toString(adapterToIndexMapping.getAdapterId())),
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(adapterToIndexMapping)));
		}
	}

	public static AdapterToIndexMapping getAdapterToIndexMapping(
			final Class<?> implementingClass,
			final JobContext context,
			final short internalAdapterId ) {
		return getAdapterToIndexMappingInternal(
				implementingClass,
				getConfiguration(context),
				internalAdapterId);
	}

	private static AdapterToIndexMapping getAdapterToIndexMappingInternal(
			final Class<?> implementingClass,
			final Configuration configuration,
			final short internalAdapterId ) {
		final String input = configuration.get(enumToConfKey(
				implementingClass,
				GeoWaveConfg.ADAPTER_TO_INDEX,
				Short.toString(internalAdapterId)));
		if (input != null) {
			final byte[] dataAdapterBytes = ByteArrayUtils.byteArrayFromString(input);
			return (AdapterToIndexMapping) PersistenceUtils.fromBinary(dataAdapterBytes);
		}
		return null;
	}

	public static void addDataAdapter(
			final Class<?> implementingClass,
			final Configuration conf,
			final DataTypeAdapter<?> adapter ) {
		if (adapter != null) {
			conf.set(
					enumToConfKey(
							implementingClass,
							GeoWaveConfg.DATA_ADAPTER,
							adapter.getTypeName()),
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(adapter)));
		}
	}

	public static void removeDataAdapter(
			final Class<?> implementingClass,
			final Configuration conf,
			final String typeName ) {
		if (typeName != null) {
			conf.unset(enumToConfKey(
					implementingClass,
					GeoWaveConfg.DATA_ADAPTER,
					typeName));
		}
	}

	public static DataTypeAdapter<?> getDataAdapter(
			final Class<?> implementingClass,
			final JobContext context,
			final String typeName ) {
		return getDataAdapterInternal(
				implementingClass,
				getConfiguration(context),
				typeName);
	}

	private static DataTypeAdapter<?> getDataAdapterInternal(
			final Class<?> implementingClass,
			final Configuration configuration,
			final String typeName ) {
		final String input = configuration.get(enumToConfKey(
				implementingClass,
				GeoWaveConfg.DATA_ADAPTER,
				typeName));
		if (input != null) {
			final byte[] dataAdapterBytes = ByteArrayUtils.byteArrayFromString(input);
			return (DataTypeAdapter<?>) PersistenceUtils.fromBinary(dataAdapterBytes);
		}
		return null;
	}

	public static DataTypeAdapter<?>[] getDataAdapters(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getDataAdaptersInternal(
				implementingClass,
				getConfiguration(context));
	}

	private static Map<String, String> getStoreOptionsMapInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		final String prefix = enumToConfKey(
				implementingClass,
				GeoWaveConfg.STORE_CONFIG_OPTION) + KEY_SEPARATOR;
		final Map<String, String> enumMap = configuration.getValByRegex(prefix + "*");
		final Map<String, String> retVal = new HashMap<>();
		for (final Entry<String, String> entry : enumMap.entrySet()) {
			final String key = entry.getKey();
			retVal.put(
					key.substring(prefix.length()),
					entry.getValue());
		}
		return retVal;
	}

	private static DataTypeAdapter<?>[] getDataAdaptersInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		final Map<String, String> input = configuration.getValByRegex(enumToConfKey(
				implementingClass,
				GeoWaveConfg.DATA_ADAPTER) + "*");
		if (input != null) {
			final List<DataTypeAdapter<?>> adapters = new ArrayList<>(
					input.size());
			for (final String dataAdapterStr : input.values()) {
				final byte[] dataAdapterBytes = ByteArrayUtils.byteArrayFromString(dataAdapterStr);
				adapters.add((DataTypeAdapter<?>) PersistenceUtils.fromBinary(dataAdapterBytes));
			}
			return adapters.toArray(new DataTypeAdapter[adapters.size()]);
		}
		return new DataTypeAdapter[] {};
	}

	private static Index getIndexInternal(
			final Class<?> implementingClass,
			final Configuration configuration,
			final String indexName ) {
		final String input = configuration.get(enumToConfKey(
				implementingClass,
				GeoWaveConfg.INDEX,
				indexName));
		if (input != null) {
			final byte[] indexBytes = ByteArrayUtils.byteArrayFromString(input);
			return (Index) PersistenceUtils.fromBinary(indexBytes);
		}
		return null;
	}

	public static Index[] getIndices(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getIndicesInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static IndexStore getJobContextIndexStore(
			final Class<?> implementingClass,
			final JobContext context ) {
		final Map<String, String> configOptions = getStoreOptionsMap(
				implementingClass,
				context);
		return new JobContextIndexStore(
				context,
				GeoWaveStoreFinder.createIndexStore(configOptions));
	}

	public static TransientAdapterStore getJobContextAdapterStore(
			final Class<?> implementingClass,
			final JobContext context ) {
		final Map<String, String> configOptions = getStoreOptionsMap(
				implementingClass,
				context);
		return new JobContextAdapterStore(
				context,
				GeoWaveStoreFinder.createAdapterStore(configOptions),
				getJobContextInternalAdapterStore(
						implementingClass,
						context));
	}

	public static AdapterIndexMappingStore getJobContextAdapterIndexMappingStore(
			final Class<?> implementingClass,
			final JobContext context ) {
		final Map<String, String> configOptions = getStoreOptionsMap(
				implementingClass,
				context);
		return new JobContextAdapterIndexMappingStore(
				context,
				GeoWaveStoreFinder.createAdapterIndexMappingStore(configOptions));
	}

	public static InternalAdapterStore getJobContextInternalAdapterStore(
			final Class<?> implementingClass,
			final JobContext context ) {
		final Map<String, String> configOptions = getStoreOptionsMap(
				implementingClass,
				context);
		return new JobContextInternalAdapterStore(
				context,
				GeoWaveStoreFinder.createInternalAdapterStore(configOptions));
	}

	private static Index[] getIndicesInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		final Map<String, String> input = configuration.getValByRegex(enumToConfKey(
				implementingClass,
				GeoWaveConfg.INDEX) + "*");
		if (input != null) {
			final List<Index> indices = new ArrayList<>(
					input.size());
			for (final String indexStr : input.values()) {
				final byte[] indexBytes = ByteArrayUtils.byteArrayFromString(indexStr);
				indices.add((Index) PersistenceUtils.fromBinary(indexBytes));
			}
			return indices.toArray(new Index[indices.size()]);
		}
		return new Index[] {};
	}

	// use reflection to pull the Configuration out of the JobContext for Hadoop
	// 1 and Hadoop 2 compatibility
	public static Configuration getConfiguration(
			final JobContext context ) {
		try {
			final Class<?> c = GeoWaveConfiguratorBase.class.getClassLoader().loadClass(
					"org.apache.hadoop.mapreduce.JobContext");
			final Method m = c.getMethod("getConfiguration");
			final Object o = m.invoke(
					context,
					new Object[0]);
			return (Configuration) o;
		}
		catch (final Exception e) {
			throw new RuntimeException(
					e);
		}
	}

	public static void setRemoteInvocationParams(
			final String hdfsHostPort,
			final String jobTrackerOrResourceManagerHostPort,
			final Configuration conf )
			throws IOException {
		String finalHdfsHostPort;
		// Ensures that the url starts with hdfs://
		if (!hdfsHostPort.contains("://")) {
			finalHdfsHostPort = "hdfs://" + hdfsHostPort;
		}
		else {
			finalHdfsHostPort = hdfsHostPort;
		}

		conf.set(
				"fs.defaultFS",
				finalHdfsHostPort);
		conf.set(
				"fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

		// if this property is used, it hadoop does not support yarn
		conf.set(
				"mapreduce.jobtracker.address",
				jobTrackerOrResourceManagerHostPort);
		// the following 3 properties will only be used if the hadoop version
		// does support yarn
		if ("local".equals(jobTrackerOrResourceManagerHostPort)) {
			conf.set(
					"mapreduce.framework.name",
					"local");
		}
		else {
			conf.set(
					"mapreduce.framework.name",
					"yarn");
		}
		conf.set(
				"yarn.resourcemanager.address",
				jobTrackerOrResourceManagerHostPort);
		// if remotely submitted with yarn, the job configuration xml will be
		// written to this staging directory, it is generally good practice to
		// ensure the staging directory is different for each user
		String user = System.getProperty("user.name");
		if ((user == null) || user.isEmpty()) {
			user = "default";
		}
		conf.set(
				"yarn.app.mapreduce.am.staging-dir",
				"/tmp/hadoop-" + user);

	}
}
