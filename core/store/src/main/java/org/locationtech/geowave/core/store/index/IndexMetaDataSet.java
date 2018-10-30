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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.SortedIndexStrategy;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.IndexStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.IndexStatisticsType;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.callback.DeleteCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.util.DataStoreUtils;

public class IndexMetaDataSet<T> extends
		AbstractDataStatistics<T, List<IndexMetaData>, IndexStatisticsQueryBuilder<List<IndexMetaData>>> implements
		DeleteCallback<T, GeoWaveRow>
{
	private List<IndexMetaData> metaData;
	public static final IndexStatisticsType<List<IndexMetaData>> STATS_TYPE = new IndexStatisticsType<>(
			"INDEX_METADATA");

	public IndexMetaDataSet() {}

	private IndexMetaDataSet(
			final short internalDataAdapterId,
			final String indexName,
			final List<IndexMetaData> metaData ) {
		super(
				internalDataAdapterId,
				STATS_TYPE,
				indexName);
		this.metaData = metaData;
	}

	public IndexMetaDataSet(
			final short internalDataAdapterId,
			final String indexName,
			final SortedIndexStrategy<?, ?> indexStrategy ) {
		this(
				internalDataAdapterId,
				indexName,
				indexStrategy.createMetaData());
	}

	@Override
	public InternalDataStatistics<T, List<IndexMetaData>, IndexStatisticsQueryBuilder<List<IndexMetaData>>> duplicate() {
		return new IndexMetaDataSet<>(
				adapterId,
				extendedId,
				this.metaData);
	}

	public List<IndexMetaData> getMetaData() {
		return metaData;
	}

	@Override
	public byte[] toBinary() {
		final byte[] metaBytes = PersistenceUtils.toBinary(metaData);
		final ByteBuffer buffer = super.binaryBuffer(metaBytes.length);
		buffer.put(metaBytes);
		return buffer.array();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		final byte[] metaBytes = new byte[buffer.remaining()];
		buffer.get(metaBytes);

		metaData = (List) PersistenceUtils.fromBinaryAsList(metaBytes);
	}

	public IndexMetaData[] toArray() {
		return metaData.toArray(new IndexMetaData[metaData.size()]);
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		if ((merge != null) && (merge instanceof IndexMetaDataSet)) {
			for (int i = 0; i < metaData.size(); i++) {
				final IndexMetaData imd = metaData.get(i);
				final IndexMetaData imd2 = ((IndexMetaDataSet<T>) merge).metaData.get(i);
				imd.merge(imd2);
			}
		}
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		if (!this.metaData.isEmpty()) {
			final InsertionIds insertionIds = DataStoreUtils.keysToInsertionIds(kvs);
			for (final IndexMetaData imd : this.metaData) {
				imd.insertionIdsAdded(insertionIds);
			}
		}
	}

	@Override
	public void entryDeleted(
			final T entry,
			final GeoWaveRow... kvs ) {
		if (!this.metaData.isEmpty()) {
			final InsertionIds insertionIds = DataStoreUtils.keysToInsertionIds(kvs);
			for (final IndexMetaData imd : this.metaData) {
				imd.insertionIdsRemoved(insertionIds);
			}
		}
	}

	public static IndexMetaData[] getIndexMetadata(
			final Index index,
			final List<Short> adapterIdsToQuery,
			final DataStatisticsStore statisticsStore,
			final String... authorizations ) {
		IndexMetaDataSet combinedMetaData = null;
		for (final short adapterId : adapterIdsToQuery) {
			try (final CloseableIterator<InternalDataStatistics<?, ?, ?>> adapterMetadataIt = statisticsStore
					.getDataStatistics(
							adapterId,
							index.getName(),
							STATS_TYPE,
							authorizations)) {
				if (adapterMetadataIt.hasNext()) {
					final IndexMetaDataSet adapterMetadata = (IndexMetaDataSet) adapterMetadataIt.next();
					if (combinedMetaData == null) {
						combinedMetaData = adapterMetadata;
					}
					else {
						combinedMetaData.merge(adapterMetadata);
					}
				}
			}
		}
		return combinedMetaData != null ? combinedMetaData.toArray() : null;
	}

	@Override
	protected String resultsName() {
		return "indexHints";
	}

	@Override
	protected Object resultsValue() {
		final Collection<Object> mdArray = new ArrayList<>();
		for (final IndexMetaData imd : this.metaData) {
			mdArray.add(imd.toJSONObject());
		}
		return mdArray;
	}

	@Override
	public List<IndexMetaData> getResult() {
		return metaData;
	}

}
