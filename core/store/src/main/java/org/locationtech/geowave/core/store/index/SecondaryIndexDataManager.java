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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.callback.DeleteCallback;
import org.locationtech.geowave.core.store.callback.IngestCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.flatten.BitmaskUtils;
import org.locationtech.geowave.core.store.util.DataStoreUtils;

/**
 * One manager associated with each primary index.
 *
 *
 * @param <T>
 *            The type of entity being indexed
 */
public class SecondaryIndexDataManager<T> implements
		Closeable,
		IngestCallback<T>,
		DeleteCallback<T, GeoWaveRow>
{
	private final SecondaryIndexDataAdapter<T> adapter;
	private final SecondaryIndexDataStore secondaryIndexStore;
	private final CommonIndexModel primaryIndexModel;
	private final String primaryIndexName;

	public SecondaryIndexDataManager(
			final SecondaryIndexDataStore secondaryIndexStore,
			final SecondaryIndexDataAdapter<T> adapter,
			final Index primaryIndex ) {
		this.adapter = adapter;
		this.secondaryIndexStore = secondaryIndexStore;
		this.primaryIndexModel = primaryIndex.getIndexModel();
		this.primaryIndexName = primaryIndex.getName();

	}

	public void entryCallback(
			final T entry,
			final boolean delete,
			final GeoWaveRow... kvs ) {
		// loop secondary indices for adapter
		final InsertionIds primaryIndexInsertionIds = DataStoreUtils.keysToInsertionIds(kvs);
		for (final SecondaryIndexImpl<T> secondaryIndex : adapter.getSupportedSecondaryIndices()) {
			final String indexedAttributeFieldName = secondaryIndex.getFieldName();
			final int position = adapter.getPositionOfOrderedField(
					primaryIndexModel,
					indexedAttributeFieldName);
			Object fieldValue = null;
			byte[] visibility = null;
			// find the field value and deserialize it
			for (final GeoWaveValue v : kvs[0].getFieldValues()) {
				if (BitmaskUtils.getFieldPositions(
						v.getFieldMask()).contains(
						position)) {
					final byte[] fieldSubsetBitmask = BitmaskUtils.generateCompositeBitmask(position);

					final byte[] byteValue = BitmaskUtils.constructNewValue(
							v.getValue(),
							v.getFieldMask(),
							fieldSubsetBitmask);
					fieldValue = adapter.getReader(
							indexedAttributeFieldName).readField(
							byteValue);
					visibility = v.getVisibility();
					break;
				}
			}
			// get indexed value(s) for current field
			@SuppressWarnings("unchecked")
			final InsertionIds secondaryIndexInsertionIds = secondaryIndex.getIndexStrategy().getInsertionIds(
					fieldValue);
			// loop insertionIds
			for (final ByteArray insertionId : secondaryIndexInsertionIds.getCompositeInsertionIds()) {
				final ByteArray dataId = new ByteArray(
						kvs[0].getDataId());
				switch (secondaryIndex.getSecondaryIndexType()) {
					case JOIN:
						final Pair<ByteArray, ByteArray> firstPartitionAndSortKey = primaryIndexInsertionIds
								.getFirstPartitionAndSortKeyPair();
						if (delete) {
							secondaryIndexStore.storeJoinEntry(
									secondaryIndex.getName(),
									insertionId,
									adapter.getTypeName(),
									indexedAttributeFieldName,
									primaryIndexName,
									firstPartitionAndSortKey.getLeft(),
									firstPartitionAndSortKey.getRight(),
									new ByteArray(
											visibility));
						}
						else {
							secondaryIndexStore.deleteJoinEntry(
									secondaryIndex.getName(),
									insertionId,
									adapter.getTypeName(),
									indexedAttributeFieldName,
									primaryIndexName,
									firstPartitionAndSortKey.getLeft(),
									firstPartitionAndSortKey.getRight(),
									new ByteArray(
											visibility));
						}
						break;
					case PARTIAL:
						final List<String> attributesToStore = secondaryIndex.getPartialFieldNames();

						final byte[] fieldSubsetBitmask = BitmaskUtils.generateFieldSubsetBitmask(
								primaryIndexModel,
								attributesToStore.toArray(new String[0]),
								adapter);
						final List<GeoWaveValue> subsetValues = new ArrayList<>();
						for (final GeoWaveValue value : kvs[0].getFieldValues()) {
							byte[] byteValue = value.getValue();
							byte[] fieldMask = value.getFieldMask();

							if (fieldSubsetBitmask != null) {
								final byte[] newBitmask = BitmaskUtils.generateANDBitmask(
										fieldMask,
										fieldSubsetBitmask);
								byteValue = BitmaskUtils.constructNewValue(
										byteValue,
										fieldMask,
										newBitmask);
								if ((byteValue == null) || (byteValue.length == 0)) {
									continue;
								}
								fieldMask = newBitmask;
							}
							subsetValues.add(new GeoWaveValueImpl(
									fieldMask,
									value.getVisibility(),
									byteValue));
						}
						secondaryIndexStore.storeEntry(
								secondaryIndex.getName(),
								insertionId,
								adapter.getTypeName(),
								indexedAttributeFieldName,
								dataId,
								subsetValues.toArray(new GeoWaveValue[] {}));
						break;
					case FULL:
						// assume multiple rows are duplicates, so just take the
						// first one

						secondaryIndexStore.storeEntry(
								secondaryIndex.getName(),
								insertionId,
								adapter.getTypeName(),
								indexedAttributeFieldName,
								dataId,
								// full simply sends over all of the
								// attributes
								// kvs is gauranteed to be at least one, or
								// there would have been nothing ingested
								kvs[0].getFieldValues());
						break;
					default:
						break;
				}
			}
			if (delete) {
				// capture statistics
				for (final InternalDataStatistics<T, ?, ?> associatedStatistic : secondaryIndex
						.getAssociatedStatistics()) {
					associatedStatistic.entryIngested(
							entry,
							kvs);
				}
			}
		}
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		entryCallback(
				entry,
				false,
				kvs);
	}

	@Override
	public void entryDeleted(
			final T entry,
			final GeoWaveRow... kvs ) {
		entryCallback(
				entry,
				true,
				kvs);

	}

	@Override
	public void close()
			throws IOException {
		if (secondaryIndexStore != null) {
			secondaryIndexStore.flush();
		}
	}

}
