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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.Transformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.IndexStore;

/**
 * This class implements an index store by first checking the job context for an
 * index and keeping a local cache of indices that have been discovered. It will
 * check the metadata store if it cannot find an index in the job context.
 */
public class JobContextIndexStore implements
		IndexStore
{
	private static final Class<?> CLASS = JobContextIndexStore.class;
	private final JobContext context;
	private final IndexStore persistentIndexStore;
	private final Map<String, Index> indexCache = new HashMap<>();

	public JobContextIndexStore(
			final JobContext context,
			final IndexStore persistentIndexStore ) {
		this.context = context;
		this.persistentIndexStore = persistentIndexStore;
	}

	@Override
	public void addIndex(
			final Index index ) {
		indexCache.put(
				index.getName(),
				index);
	}

	@Override
	public Index getIndex(
			final String indexName ) {
		Index index = indexCache.get(indexName);
		if (index == null) {
			index = getIndexInternal(indexName);
		}
		return index;
	}

	@Override
	public boolean indexExists(
			final String indexName ) {
		if (indexCache.containsKey(indexName)) {
			return true;
		}
		final Index index = getIndexInternal(indexName);
		return index != null;
	}

	private Index getIndexInternal(
			final String indexName ) {
		// first try to get it from the job context
		Index index = getIndex(
				context,
				indexName);
		if (index == null) {
			// then try to get it from the accumulo persistent store
			index = persistentIndexStore.getIndex(indexName);
		}

		if (index != null) {
			indexCache.put(
					indexName,
					index);
		}
		return index;
	}

	@Override
	public void removeAll() {
		indexCache.clear();
	}

	@Override
	public CloseableIterator<Index> getIndices() {
		final CloseableIterator<Index> it = persistentIndexStore.getIndices();
		// cache any results
		return new CloseableIteratorWrapper<Index>(
				it,
				IteratorUtils.transformedIterator(
						it,
						new Transformer() {

							@Override
							public Object transform(
									final Object obj ) {
								indexCache.put(
										((Index) obj).getName(),
										(Index) obj);
								return obj;
							}
						}));
	}

	public static void addIndex(
			final Configuration config,
			final Index index ) {
		GeoWaveConfiguratorBase.addIndex(
				CLASS,
				config,
				index);
	}

	protected static Index getIndex(
			final JobContext context,
			final String indexName ) {
		return GeoWaveConfiguratorBase.getIndex(
				CLASS,
				context,
				indexName);
	}

	public static Index[] getIndices(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getIndices(
				CLASS,
				context);
	}

}
