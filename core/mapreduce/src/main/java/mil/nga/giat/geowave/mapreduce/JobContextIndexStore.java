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
package mil.nga.giat.geowave.mapreduce;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.Transformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

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
	private final Map<ByteArrayId, Index<?, ?>> indexCache = new HashMap<ByteArrayId, Index<?, ?>>();

	public JobContextIndexStore(
			final JobContext context,
			final IndexStore persistentIndexStore ) {
		this.context = context;
		this.persistentIndexStore = persistentIndexStore;
	}

	@Override
	public void addIndex(
			final Index<?, ?> index ) {
		indexCache.put(
				index.getId(),
				index);
	}

	@Override
	public Index<?, ?> getIndex(
			final ByteArrayId indexId ) {
		Index<?, ?> index = indexCache.get(indexId);
		if (index == null) {
			index = getIndexInternal(indexId);
		}
		return index;
	}

	@Override
	public boolean indexExists(
			final ByteArrayId indexId ) {
		if (indexCache.containsKey(indexId)) {
			return true;
		}
		final Index<?, ?> index = getIndexInternal(indexId);
		return index != null;
	}

	private Index<?, ?> getIndexInternal(
			final ByteArrayId indexId ) {
		// first try to get it from the job context
		Index<?, ?> index = getIndex(
				context,
				indexId);
		if (index == null) {
			// then try to get it from the accumulo persistent store
			index = persistentIndexStore.getIndex(indexId);
		}

		if (index != null) {
			indexCache.put(
					indexId,
					index);
		}
		return index;
	}

	@Override
	public void removeAll() {
		indexCache.clear();
	}

	@Override
	public CloseableIterator<Index<?, ?>> getIndices() {
		final CloseableIterator<Index<?, ?>> it = persistentIndexStore.getIndices();
		// cache any results
		return new CloseableIteratorWrapper<Index<?, ?>>(
				it,
				IteratorUtils.transformedIterator(
						it,
						new Transformer() {

							@Override
							public Object transform(
									final Object obj ) {
								if (obj instanceof Index<?, ?>) {
									indexCache.put(
											((Index<?, ?>) obj).getId(),
											(Index<?, ?>) obj);
								}
								return obj;
							}
						}));
	}

	public static void addIndex(
			final Configuration config,
			final PrimaryIndex index ) {
		GeoWaveConfiguratorBase.addIndex(
				CLASS,
				config,
				index);
	}

	protected static PrimaryIndex getIndex(
			final JobContext context,
			final ByteArrayId indexId ) {
		return GeoWaveConfiguratorBase.getIndex(
				CLASS,
				context,
				indexId);
	}

	public static PrimaryIndex[] getIndices(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getIndices(
				CLASS,
				context);
	}

}
