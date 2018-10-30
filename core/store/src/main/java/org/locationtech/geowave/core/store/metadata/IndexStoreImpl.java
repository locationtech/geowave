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
package org.locationtech.geowave.core.store.metadata;

import java.util.HashMap;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataType;

/**
 * This class will persist Index objects within an Accumulo table for GeoWave
 * metadata. The indices will be persisted in an "INDEX" column family.
 *
 * There is an LRU cache associated with it so staying in sync with external
 * updates is not practical - it assumes the objects are not updated often or at
 * all. The objects are stored in their own table.
 *
 **/
public class IndexStoreImpl extends
		AbstractGeoWavePersistence<Index> implements
		IndexStore
{
	public IndexStoreImpl(
			final DataStoreOperations operations,
			final DataStoreOptions options ) {
		super(
				operations,
				options,
				MetadataType.INDEX);
	}

	@Override
	public void addIndex(
			final Index index ) {
		addObject(index);
	}

	@Override
	public Index getIndex(
			final String indexName ) {
		return getObject(
				new ByteArray(
						indexName),
				null);
	}

	@Override
	protected ByteArray getPrimaryId(
			final Index persistedObject ) {
		return new ByteArray(
				persistedObject.getName());
	}

	@Override
	public boolean indexExists(
			final String indexName ) {
		return objectExists(
				new ByteArray(
						indexName),
				null);
	}

	@Override
	public CloseableIterator<Index> getIndices() {
		return getObjects();
	}

}
