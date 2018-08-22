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

import java.nio.ByteBuffer;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.SortedIndexStrategy;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class IndexMetaDataSet<T> extends
		AbstractDataStatistics<T> implements
		DeleteCallback<T, GeoWaveRow>
{
	private List<IndexMetaData> metaData;
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"INDEX_METADATA");

	public IndexMetaDataSet() {}

	private IndexMetaDataSet(
			final short internalDataAdapterId,
			final ByteArrayId statisticsId,
			final List<IndexMetaData> metaData ) {
		super(
				internalDataAdapterId,
				composeId(statisticsId));
		this.metaData = metaData;
	}

	public IndexMetaDataSet(
			final short internalDataAdapterId,
			final ByteArrayId statisticsId,
			final SortedIndexStrategy<?, ?> indexStrategy ) {
		super(
				internalDataAdapterId,
				composeId(statisticsId));
		this.metaData = indexStrategy.createMetaData();
	}

	public static ByteArrayId composeId(
			final ByteArrayId statisticsId ) {
		return composeId(
				STATS_TYPE.getString(),
				statisticsId.getString());
	}

	@Override
	public DataStatistics<T> duplicate() {
		return new IndexMetaDataSet<T>(
				internalDataAdapterId,
				statisticsId,
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
			final PrimaryIndex index,
			final List<Short> adapterIdsToQuery,
			final DataStatisticsStore statisticsStore,
			final String... authorizations ) {
		IndexMetaDataSet combinedMetaData = null;
		for (final short adapterId : adapterIdsToQuery) {
			final IndexMetaDataSet adapterMetadata = (IndexMetaDataSet) statisticsStore.getDataStatistics(
					adapterId,
					IndexMetaDataSet.composeId(index.getId()),
					authorizations);
			if (combinedMetaData == null) {
				combinedMetaData = adapterMetadata;
			}
			else {
				combinedMetaData.merge(adapterMetadata);
			}
		}
		return combinedMetaData != null ? combinedMetaData.toArray() : null;
	}

	/**
	 * Convert Index Metadata statistics to a JSON object
	 */

	@Override
	public JSONObject toJSONObject(
			final InternalAdapterStore store )
			throws JSONException {
		final JSONObject jo = new JSONObject();
		jo.put(
				"type",
				STATS_TYPE.getString());
		jo.put(
				"dataAdapterID",
				store.getAdapterId(internalDataAdapterId));
		jo.put(
				"statisticsID",
				statisticsId.getString());

		final JSONArray mdArray = new JSONArray();
		for (final IndexMetaData imd : this.metaData) {
			mdArray.add(imd.toJSONObject());
		}
		jo.put(
				"metadata",
				mdArray);

		return jo;
	}
}
