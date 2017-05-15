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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;

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
		DeleteCallback<T>
{
	private final SecondaryIndexDataAdapter<T> adapter;
	final SecondaryIndexDataStore secondaryIndexStore;
	final ByteArrayId primaryIndexId;

	public SecondaryIndexDataManager(
			final SecondaryIndexDataStore secondaryIndexStore,
			final SecondaryIndexDataAdapter<T> adapter,
			final ByteArrayId primaryIndexId ) {
		this.adapter = adapter;
		this.secondaryIndexStore = secondaryIndexStore;
		this.primaryIndexId = primaryIndexId;

	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		// loop secondary indices for adapter
		for (final SecondaryIndex<T> secondaryIndex : adapter.getSupportedSecondaryIndices()) {
			final ByteArrayId indexedAttributeFieldId = secondaryIndex.getFieldId();
			// get fieldInfo for fieldId to be indexed
			final FieldInfo<?> indexedAttributeFieldInfo = getFieldInfo(
					entryInfo,
					indexedAttributeFieldId);
			// get indexed value(s) for current field
			@SuppressWarnings("unchecked")
			final List<ByteArrayId> secondaryIndexInsertionIds = secondaryIndex.getIndexStrategy().getInsertionIds(
					Arrays.asList(indexedAttributeFieldInfo));
			// loop insertionIds
			for (final ByteArrayId insertionId : secondaryIndexInsertionIds) {
				final ByteArrayId primaryIndexRowId = entryInfo.getRowIds().get(
						0);
				final ByteArrayId attributeVisibility = new ByteArrayId(
						indexedAttributeFieldInfo.getVisibility());
				final ByteArrayId dataId = new ByteArrayId(
						entryInfo.getDataId());
				switch (secondaryIndex.getSecondaryIndexType()) {
					case JOIN:
						secondaryIndexStore.storeJoinEntry(
								secondaryIndex.getId(),
								insertionId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								primaryIndexId,
								primaryIndexRowId,
								attributeVisibility);
						break;
					case PARTIAL:
						final List<FieldInfo<?>> attributes = new ArrayList<>();
						final List<ByteArrayId> attributesToStore = secondaryIndex.getPartialFieldIds();
						for (final ByteArrayId fieldId : attributesToStore) {
							attributes.add(getFieldInfo(
									entryInfo,
									fieldId));
						}
						secondaryIndexStore.storeEntry(
								secondaryIndex.getId(),
								insertionId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								dataId,
								attributeVisibility,
								attributes);
						break;
					case FULL:
						secondaryIndexStore.storeEntry(
								secondaryIndex.getId(),
								insertionId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								dataId,
								attributeVisibility,
								// full simply sends over all of the
								// attributes
								entryInfo.getFieldInfo());
						break;
					default:
						break;
				}
			}
			// capture statistics
			for (final DataStatistics<T> associatedStatistic : secondaryIndex.getAssociatedStatistics()) {
				associatedStatistic.entryIngested(
						entryInfo,
						entry);
			}
		}
	}

	@Override
	public void entryDeleted(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		// loop secondary indices for adapter
		for (final SecondaryIndex<T> secondaryIndex : adapter.getSupportedSecondaryIndices()) {
			final ByteArrayId indexedAttributeFieldId = secondaryIndex.getFieldId();
			// get fieldInfo for fieldId to be deleted
			final FieldInfo<?> indexedAttributeFieldInfo = getFieldInfo(
					entryInfo,
					indexedAttributeFieldId);
			// get indexed value(s) for current field
			@SuppressWarnings("unchecked")
			final List<ByteArrayId> secondaryIndexRowIds = secondaryIndex.getIndexStrategy().getInsertionIds(
					Arrays.asList(indexedAttributeFieldInfo));
			// loop insertionIds
			for (final ByteArrayId secondaryIndexRowId : secondaryIndexRowIds) {
				final ByteArrayId primaryIndexRowId = entryInfo.getRowIds().get(
						0);
				final ByteArrayId dataId = new ByteArrayId(
						entryInfo.getDataId());
				switch (secondaryIndex.getSecondaryIndexType()) {
					case JOIN:
						secondaryIndexStore.deleteJoinEntry(
								secondaryIndex.getId(),
								secondaryIndexRowId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								primaryIndexId,
								primaryIndexRowId);
						break;
					case PARTIAL:
						final List<FieldInfo<?>> attributes = new ArrayList<>();
						final List<ByteArrayId> attributesToDelete = secondaryIndex.getPartialFieldIds();
						for (final ByteArrayId fieldId : attributesToDelete) {
							attributes.add(getFieldInfo(
									entryInfo,
									fieldId));
						}
						secondaryIndexStore.deleteEntry(
								secondaryIndex.getId(),
								secondaryIndexRowId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								dataId,
								attributes);
						break;
					case FULL:
						secondaryIndexStore.deleteEntry(
								secondaryIndex.getId(),
								secondaryIndexRowId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								dataId,
								// full simply sends over all of the
								// attributes
								entryInfo.getFieldInfo());
						break;
					default:
						break;
				}
			}
			// TODO delete statistics
			// for (final DataStatistics<T> associatedStatistic :
			// secondaryIndex.getAssociatedStatistics()) {
			// associatedStatistic.entryDeleted(
			// entryInfo,
			// entry);
			// }
		}

	}

	private FieldInfo<?> getFieldInfo(
			final DataStoreEntryInfo entryInfo,
			final ByteArrayId fieldID ) {
		for (final FieldInfo<?> info : entryInfo.getFieldInfo()) {
			if (info.getDataValue().getId().equals(
					fieldID)) {
				return info;
			}
		}
		return null;
	}

	@Override
	public void close()
			throws IOException {
		if (secondaryIndexStore != null) {
			secondaryIndexStore.flush();
		}
	}

}
