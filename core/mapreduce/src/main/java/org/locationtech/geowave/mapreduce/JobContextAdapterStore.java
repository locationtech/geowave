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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.Transformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * This class implements an adapter store by first checking the job context for
 * an adapter and keeping a local cache of adapters that have been discovered.
 * It will check the metadata store if it cannot find an adapter in the job
 * context.
 */
public class JobContextAdapterStore implements
		TransientAdapterStore
{
	private static final Class<?> CLASS = JobContextAdapterStore.class;
	private final JobContext context;
	private PersistentAdapterStore persistentAdapterStore = null;
	private InternalAdapterStore internalAdapterStore = null;
	private final Map<String, DataTypeAdapter<?>> adapterCache = new HashMap<>();

	public JobContextAdapterStore(
			final JobContext context,
			final PersistentAdapterStore persistentAdapterStore,
			final InternalAdapterStore internalAdapterStore ) {
		this.context = context;
		this.persistentAdapterStore = persistentAdapterStore;
		this.internalAdapterStore = internalAdapterStore;

	}

	@Override
	public void addAdapter(
			final DataTypeAdapter<?> adapter ) {
		adapterCache.put(
				adapter.getTypeName(),
				adapter);
	}

	@Override
	public void removeAdapter(
			final String typeName ) {
		adapterCache.remove(typeName);
	}

	@Override
	public DataTypeAdapter<?> getAdapter(
			final String typeName ) {
		DataTypeAdapter<?> adapter = adapterCache.get(typeName);
		if (adapter == null) {
			adapter = getAdapterInternal(typeName);
		}
		return adapter;
	}

	@Override
	public boolean adapterExists(
			final String typeName ) {
		if (adapterCache.containsKey(typeName)) {
			return true;
		}
		final DataTypeAdapter<?> adapter = getAdapterInternal(typeName);
		return adapter != null;
	}

	private DataTypeAdapter<?> getAdapterInternal(
			final String typeName ) {
		// first try to get it from the job context
		DataTypeAdapter<?> adapter = getDataAdapter(
				context,
				typeName);
		if (adapter == null) {

			// then try to get it from the persistent store
			adapter = persistentAdapterStore.getAdapter(internalAdapterStore.getAdapterId(typeName));
		}

		if (adapter != null) {
			adapterCache.put(
					typeName,
					adapter);
		}
		return adapter;
	}

	@Override
	public void removeAll() {
		adapterCache.clear();
	}

	@Override
	public CloseableIterator<DataTypeAdapter<?>> getAdapters() {
		final CloseableIterator<InternalDataAdapter<?>> it = persistentAdapterStore.getAdapters();
		// cache any results
		return new CloseableIteratorWrapper<DataTypeAdapter<?>>(
				it,
				IteratorUtils.transformedIterator(
						it,
						new Transformer() {

							@Override
							public Object transform(
									final Object obj ) {
								if (obj instanceof DataTypeAdapter) {
									adapterCache.put(
											((DataTypeAdapter) obj).getTypeName(),
											(DataTypeAdapter) obj);
								}
								return obj;
							}
						}));
	}

	public List<String> getTypeNames() {
		final DataTypeAdapter<?>[] userAdapters = GeoWaveConfiguratorBase.getDataAdapters(
				CLASS,
				context);
		if ((userAdapters == null) || (userAdapters.length <= 0)) {
			return IteratorUtils.toList(IteratorUtils.transformedIterator(
					getAdapters(),
					new Transformer() {

						@Override
						public Object transform(
								final Object input ) {
							if (input instanceof DataTypeAdapter) {
								return ((DataTypeAdapter) input).getTypeName();
							}
							return input;
						}
					}));
		}
		else {
			final List<String> retVal = new ArrayList<>(
					userAdapters.length);
			for (final DataTypeAdapter<?> adapter : userAdapters) {
				retVal.add(adapter.getTypeName());
			}
			return retVal;
		}
	}

	protected static DataTypeAdapter<?> getDataAdapter(
			final JobContext context,
			final String typeName ) {
		return GeoWaveConfiguratorBase.getDataAdapter(
				CLASS,
				context,
				typeName);
	}

	public static DataTypeAdapter<?>[] getDataAdapters(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getDataAdapters(
				CLASS,
				context);
	}

	public static void addDataAdapter(
			final Configuration configuration,
			final DataTypeAdapter<?> adapter ) {
		GeoWaveConfiguratorBase.addDataAdapter(
				CLASS,
				configuration,
				adapter);
	}

	public static void removeAdapter(
			final Configuration configuration,
			final String typeName ) {
		GeoWaveConfiguratorBase.removeDataAdapter(
				CLASS,
				configuration,
				typeName);
	}
}
