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
package org.locationtech.geowave.core.store.memory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.IndexStore;

/**
 * This is a simple HashMap based in-memory implementation of the IndexStore and
 * can be useful if it is undesirable to persist and query objects within
 * another storage mechanism such as an accumulo table.
 */
public class MemoryIndexStore implements
		IndexStore
{
	private final Map<String, Index> indexMap = Collections.synchronizedMap(new HashMap<String, Index>());

	public MemoryIndexStore() {}

	public MemoryIndexStore(
			final Index[] initialIndices ) {
		for (final Index index : initialIndices) {
			addIndex(index);
		}
	}

	@Override
	public void addIndex(
			final Index index ) {
		indexMap.put(
				index.getName(),
				index);
	}

	@Override
	public Index getIndex(
			final String indexName ) {
		return indexMap.get(indexName);
	}

	@Override
	public boolean indexExists(
			final String indexName ) {
		return indexMap.containsKey(indexName);
	}

	@Override
	public CloseableIterator<Index> getIndices() {
		return new CloseableIterator.Wrapper<>(
				new ArrayList<>(
						indexMap.values()).iterator());
	}

	@Override
	public void removeAll() {
		indexMap.clear();
	}

}
