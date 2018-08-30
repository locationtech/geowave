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
package mil.nga.giat.geowave.core.store.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;

public class DataAdapterAndIndexCache
{

	private static Map<String, DataAdapterAndIndexCache> CACHE_MAP = new HashMap<String, DataAdapterAndIndexCache>();

	public static synchronized DataAdapterAndIndexCache getInstance(
			final String cacheId,
			final String gwNamespace,
			final String storeType ) {
		final String qualifiedId = (((gwNamespace != null) && !gwNamespace.isEmpty()) ? cacheId + "_" + gwNamespace
				: cacheId) + "_" + storeType;
		DataAdapterAndIndexCache instance = CACHE_MAP.get(qualifiedId);
		if (instance == null) {
			instance = new DataAdapterAndIndexCache();
			CACHE_MAP.put(
					qualifiedId,
					instance);
		}
		return instance;
	}

	private final Set<DataAdapterAndIndex> cache = new HashSet<DataAdapterAndIndex>();

	// TODO: there should techinically be a notion of geowave datastore in here,
	// as multiple different datastores (perhaps simply different gwNamespaces)
	// could use the same adapter and index
	public synchronized boolean add(
			final short internalAdapterId,
			final String indexId ) {
		if (cache.contains(new DataAdapterAndIndex(
				internalAdapterId,
				indexId))) {
			return true;
		}
		else {
			cache.add(new DataAdapterAndIndex(
					internalAdapterId,
					indexId));
			return false;
		}
	}

	public synchronized void deleteIndex(
			final String indexId ) {
		final Iterator<DataAdapterAndIndex> it = cache.iterator();
		while (it.hasNext()) {
			if (indexId.equals(it.next().indexId)) {
				it.remove();
			}
		}
	}

	public synchronized void deleteAll() {
		cache.clear();
	}

	private static class DataAdapterAndIndex
	{
		private final short internalAdapterId;
		private final String indexId;

		public DataAdapterAndIndex(
				final short internalAdapterId,
				final String indexId ) {
			this.internalAdapterId = internalAdapterId;
			this.indexId = indexId;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((indexId == null) ? 0 : indexId.hashCode());
			result = prime * result + internalAdapterId;
			return result;
		}

		@Override
		public boolean equals(
				Object obj ) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			DataAdapterAndIndex other = (DataAdapterAndIndex) obj;
			if (indexId == null) {
				if (other.indexId != null) return false;
			}
			else if (!indexId.equals(other.indexId)) return false;
			if (internalAdapterId != other.internalAdapterId) return false;
			return true;
		}
	}
}
