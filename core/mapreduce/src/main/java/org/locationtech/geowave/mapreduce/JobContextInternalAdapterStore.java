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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

public class JobContextInternalAdapterStore implements
		InternalAdapterStore
{
	private static final Class<?> CLASS = JobContextInternalAdapterStore.class;
	private final JobContext context;
	private final InternalAdapterStore persistentInternalAdapterStore;
	protected final BiMap<String, Short> cache = HashBiMap.create();

	public JobContextInternalAdapterStore(
			final JobContext context,
			final InternalAdapterStore persistentInternalAdapterStore ) {
		this.context = context;
		this.persistentInternalAdapterStore = persistentInternalAdapterStore;
	}

	@Override
	public String getTypeName(
			final short adapterId ) {
		String typeName = cache.inverse().get(
				adapterId);
		if (typeName == null) {
			typeName = getTypeNameIInternal(adapterId);
		}
		return typeName;
	}

	private String getTypeNameIInternal(
			final short adapterId ) {
		// first try to get it from the job context
		String typeName = getAdapterIdFromJobContext(adapterId);
		if (typeName == null) {
			// then try to get it from the persistent store
			typeName = persistentInternalAdapterStore.getTypeName(adapterId);
		}

		if (typeName != null) {
			cache.put(
					typeName,
					adapterId);
		}
		return typeName;
	}

	private Short getAdapterIdInternal(
			final String typeName ) {
		// first try to get it from the job context
		Short internalAdapterId = getAdapterIdFromJobContext(typeName);
		if (internalAdapterId == null) {
			// then try to get it from the persistent store
			internalAdapterId = persistentInternalAdapterStore.getAdapterId(typeName);
		}

		if (internalAdapterId != null) {
			cache.put(
					typeName,
					internalAdapterId);
		}
		return internalAdapterId;
	}

	@Override
	public Short getAdapterId(
			final String typeName ) {
		Short internalAdapterId = cache.get(typeName);
		if (internalAdapterId == null) {
			internalAdapterId = getAdapterIdInternal(typeName);
		}
		return internalAdapterId;
	}

	protected Short getAdapterIdFromJobContext(
			final String typeName ) {
		return GeoWaveConfiguratorBase.getAdapterId(
				CLASS,
				context,
				typeName);
	}

	protected String getAdapterIdFromJobContext(
			final short internalAdapterId ) {
		return GeoWaveConfiguratorBase.getTypeName(
				CLASS,
				context,
				internalAdapterId);
	}

	@Override
	public short addTypeName(
			final String typeName ) {
		return persistentInternalAdapterStore.addTypeName(typeName);
	}

	@Override
	public boolean remove(
			final String typeName ) {
		return persistentInternalAdapterStore.remove(typeName);
	}

	public static void addTypeName(
			final Configuration configuration,
			final String typeName,
			final short adapterId ) {
		GeoWaveConfiguratorBase.addTypeName(
				CLASS,
				configuration,
				typeName,
				adapterId);
	}

	@Override
	public boolean remove(
			final short adapterId ) {
		cache.inverse().remove(
				adapterId);
		return persistentInternalAdapterStore.remove(adapterId);
	}

	@Override
	public void removeAll() {
		cache.clear();
		persistentInternalAdapterStore.removeAll();
	}

	@Override
	public String[] getTypeNames() {
		return persistentInternalAdapterStore.getTypeNames();
	}

	@Override
	public short[] getAdapterIds() {
		return persistentInternalAdapterStore.getAdapterIds();
	}

}
