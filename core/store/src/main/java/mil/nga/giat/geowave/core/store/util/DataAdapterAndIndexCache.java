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
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;

public class DataAdapterAndIndexCache
{
	private static Map<String, DataAdapterAndIndexCache> CACHE_MAP = new HashMap<String, DataAdapterAndIndexCache>();

	public static synchronized DataAdapterAndIndexCache getInstance(
			final String cacheId ) {
		DataAdapterAndIndexCache instance = CACHE_MAP.get(cacheId);
		if (instance == null) {
			instance = new DataAdapterAndIndexCache();
			CACHE_MAP.put(
					cacheId,
					instance);
		}
		return instance;
	}

	private final Set<DataAdapterAndIndex> cache = new HashSet<DataAdapterAndIndex>();

	public synchronized boolean add(
			final ByteArrayId adapterId,
			final String indexId ) {
		if (cache.contains(new DataAdapterAndIndex(
				adapterId,
				indexId))) {
			return true;
		}
		else {
			cache.add(new DataAdapterAndIndex(
					adapterId,
					indexId));
			return false;
		}
	}

	private static class DataAdapterAndIndex
	{
		private final ByteArrayId adapterId;
		private final String indexId;

		public DataAdapterAndIndex(
				final ByteArrayId adapterId,
				final String indexId ) {
			this.adapterId = adapterId;
			this.indexId = indexId;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((adapterId == null) ? 0 : adapterId.hashCode());
			result = (prime * result) + ((indexId == null) ? 0 : indexId.hashCode());
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final DataAdapterAndIndex other = (DataAdapterAndIndex) obj;
			if (adapterId == null) {
				if (other.adapterId != null) {
					return false;
				}
			}
			else if (!adapterId.equals(other.adapterId)) {
				return false;
			}
			if (indexId == null) {
				if (other.indexId != null) {
					return false;
				}
			}
			else if (!indexId.equals(other.indexId)) {
				return false;
			}
			return true;
		}
	}
}
