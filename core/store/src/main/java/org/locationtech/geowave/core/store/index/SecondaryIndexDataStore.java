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
package org.locationtech.geowave.core.store.index;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;

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
	 * @param secondaryIndexName
	 * @param indexedAttributeValue
	 * @param typeName
	 * @param indexedAttributeFieldName
	 * @param primaryIndexName
	 * @param primaryIndexPartitionKey
	 * @param primaryIndexSortKey
	 * @param attributeVisibility
	 */
	public void storeJoinEntry(
			String secondaryIndexName,
			ByteArrayId indexedAttributeValue,
			String typeName,
			String indexedAttributeFieldName,
			String primaryIndexName,
			ByteArrayId primaryIndexPartitionKey,
			ByteArrayId primaryIndexSortKey,
			ByteArrayId attributeVisibility );

	/**
	 * Stores a secondary index entry that will not require a join against the
	 * primary index upon lookup.
	 *
	 * @param secondaryIndexName
	 * @param indexedAttributeValue
	 * @param adapterId
	 * @param indexedAttributeFieldName
	 * @param dataId
	 * @param originalFields
	 */
	public void storeEntry(
			String secondaryIndexName,
			ByteArrayId indexedAttributeValue,
			String typeName,
			String indexedAttributeFieldName,
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
			final SecondaryIndexImpl<T> secondaryIndex,
			final String indexedAttributeFieldName,
			final InternalDataAdapter<T> adapter,
			final Index primaryIndex,
			final QueryConstraints query,
			final String... authorizations );

	public void deleteJoinEntry(
			String secondaryIndexName,
			ByteArrayId indexedAttributeValue,
			String typeName,
			String indexedAttributeFieldName,
			String primaryIndexName,
			ByteArrayId primaryIndexPartitionKey,
			ByteArrayId primaryIndexSortKey,
			ByteArrayId attributeVisibility );

	public void deleteEntry(
			String secondaryIndexName,
			ByteArrayId indexedAttributeValue,
			String typeName,
			String indexedAttributeFieldName,
			ByteArrayId dataId,
			GeoWaveValue... originalFields );

	public void flush();

	public void removeAll();
}
