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
package mil.nga.giat.geowave.core.store.index;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;

/**
 * This is responsible for persisting secondary index entries
 */
public interface SecondaryIndexDataStore
{
	/**
	 * Set the reference to the primary data store
	 * 
	 * @param dataStore
	 */
	public void setDataStore(
			DataStore dataStore );

	/**
	 * Stores a secondary index entry that will require a join against the
	 * primary index upon lookup.
	 * 
	 * @param secondaryIndexId
	 * @param indexedAttributeValue
	 * @param adapterId
	 * @param indexedAttributeFieldId
	 * @param primaryIndexPartitionKey
	 * @param primaryIndexSortKey
	 * @param attributeVisibility
	 */
	public void storeJoinEntry(
			ByteArrayId secondaryIndexId,
			ByteArrayId indexedAttributeValue,
			ByteArrayId adapterId,
			ByteArrayId indexedAttributeFieldId,
			ByteArrayId primaryIndexId,
			ByteArrayId primaryIndexPartitionKey,
			ByteArrayId primaryIndexSortKey,
			ByteArrayId attributeVisibility );

	/**
	 * Stores a secondary index entry that will not require a join against the
	 * primary index upon lookup.
	 * 
	 * @param secondaryIndexId
	 * @param indexedAttributeValue
	 * @param adapterId
	 * @param indexedAttributeFieldId
	 * @param dataId
	 * @param originalFields
	 */
	public void storeEntry(
			ByteArrayId secondaryIndexId,
			ByteArrayId indexedAttributeValue,
			ByteArrayId adapterId,
			ByteArrayId indexedAttributeFieldId,
			ByteArrayId dataId,
			GeoWaveValue... originalFields );

	/**
	 * Execute a query against the given secondary index
	 * 
	 * @param secondaryIndex
	 * @param indexedAttributeFieldId
	 * @param adapter
	 * @param primaryIndex
	 * @param query
	 * @param authorizations
	 * @return
	 */
	public <T> CloseableIterator<T> query(
			final SecondaryIndex<T> secondaryIndex,
			final ByteArrayId indexedAttributeFieldId,
			final InternalDataAdapter<T> adapter,
			final PrimaryIndex primaryIndex,
			final DistributableQuery query,
			final String... authorizations );

	public void deleteJoinEntry(
			ByteArrayId secondaryIndexId,
			ByteArrayId indexedAttributeValue,
			ByteArrayId adapterId,
			ByteArrayId indexedAttributeFieldId,
			ByteArrayId primaryIndexPartitionKey,
			ByteArrayId primaryIndexSortKey,
			ByteArrayId attributeVisibility );

	public void deleteEntry(
			ByteArrayId secondaryIndexId,
			ByteArrayId indexedAttributeValue,
			ByteArrayId adapterId,
			ByteArrayId indexedAttributeFieldId,
			ByteArrayId dataId,
			GeoWaveValue... originalFields );

	public void flush();

	public void removeAll();
}
