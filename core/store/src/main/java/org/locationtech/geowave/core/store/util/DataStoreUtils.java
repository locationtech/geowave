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
package org.locationtech.geowave.core.store.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.visibility.UnconstrainedVisibilityHandler;
import org.locationtech.geowave.core.store.data.visibility.UniformVisibilityWriter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.flatten.BitmaskUtils;
import org.locationtech.geowave.core.store.flatten.FlattenedDataSet;
import org.locationtech.geowave.core.store.flatten.FlattenedFieldInfo;
import org.locationtech.geowave.core.store.flatten.FlattenedUnreadData;
import org.locationtech.geowave.core.store.flatten.FlattenedUnreadDataSingleRow;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.metadata.AbstractGeoWavePersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;
import com.google.common.collect.Iterators;

/*
 */
public class DataStoreUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(DataStoreUtils.class);

	// we append a 0 byte, 8 bytes of timestamp, and 16 bytes of UUID
	public final static int UNIQUE_ADDED_BYTES = 1 + 8 + 16;
	public final static byte UNIQUE_ID_DELIMITER = 0;
	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	public static final UniformVisibilityWriter UNCONSTRAINED_VISIBILITY = new UniformVisibilityWriter(
			new UnconstrainedVisibilityHandler());

	public static final byte[] EMTPY_VISIBILITY = new byte[] {};

	public static DataTypeAdapter getDataAdapter(
			final DataStorePluginOptions dataStore,
			final ByteArrayId adapterId ) {
		Short internId = dataStore.createInternalAdapterStore().getInternalAdapterId(
				adapterId);
		if (internId == null) {
			return null;
		}

		DataTypeAdapter adapter = dataStore.createAdapterStore().getAdapter(
				internId);
		if (adapter == null) {
			return null;
		}

		return adapter;
	}

	public static FlattenedUnreadData aggregateFieldData(
			final GeoWaveKey key,
			final GeoWaveValue value,
			final PersistentDataset<CommonIndexValue> commonData,
			final CommonIndexModel model,
			final List<ByteArrayId> commonIndexFieldIds ) {
		final byte[] fieldMask = value.getFieldMask();
		final byte[] valueBytes = value.getValue();
		final FlattenedDataSet dataSet = DataStoreUtils.decomposeFlattenedFields(
				fieldMask,
				valueBytes,
				value.getVisibility(),
				commonIndexFieldIds.size() - 1);
		final List<FlattenedFieldInfo> fieldInfos = dataSet.getFieldsRead();

		for (final FlattenedFieldInfo fieldInfo : fieldInfos) {
			final int ordinal = fieldInfo.getFieldPosition();
			if (ordinal < commonIndexFieldIds.size()) {
				final ByteArrayId commonIndexFieldId = commonIndexFieldIds.get(ordinal);
				final FieldReader<? extends CommonIndexValue> reader = model.getReader(commonIndexFieldId);
				if (reader != null) {
					final CommonIndexValue fieldValue = reader.readField(fieldInfo.getValue());
					fieldValue.setVisibility(value.getVisibility());
					commonData.addValue(
							commonIndexFieldId,
							fieldValue);
				}
				else {
					LOGGER.error("Could not find reader for common index field: " + commonIndexFieldId.getString());
				}
			}
		}
		return dataSet.getFieldsDeferred();
	}

	public static List<ByteArrayId> getUniqueDimensionFields(
			final CommonIndexModel model ) {
		final List<ByteArrayId> dimensionFieldIds = new ArrayList<>();
		for (final NumericDimensionField<? extends CommonIndexValue> dimension : model.getDimensions()) {
			if (!dimensionFieldIds.contains(dimension.getFieldId())) {
				dimensionFieldIds.add(dimension.getFieldId());
			}
		}
		return dimensionFieldIds;
	}

	public static <T> long cardinality(
			final Index index,
			final Map<ByteArrayId, InternalDataStatistics<T>> stats,
			final QueryRanges queryRanges ) {

		long count = 0;
		for (final SinglePartitionQueryRanges partitionRange : queryRanges.getPartitionQueryRanges()) {
			final RowRangeHistogramStatistics rangeStats = (RowRangeHistogramStatistics) stats
					.get(RowRangeHistogramStatistics.composeId(
							index.getId(),
							partitionRange.getPartitionKey() != null ? partitionRange.getPartitionKey()
									: new ByteArrayId()));
			if (rangeStats == null) {
				return Long.MAX_VALUE - 1;
			}
			for (final ByteArrayRange range : partitionRange.getSortKeyRanges()) {
				count += rangeStats.cardinality(
						range.getStart().getBytes(),
						range.getEnd().getBytes());
			}
		}
		return count;
	}

	public static InsertionIds keysToInsertionIds(
			final GeoWaveKey... geoWaveKeys ) {
		final Map<ByteArrayId, List<ByteArrayId>> sortKeysPerPartition = new HashMap<>();
		for (final GeoWaveKey key : geoWaveKeys) {
			final ByteArrayId partitionKey = new ByteArrayId(
					key.getPartitionKey());
			List<ByteArrayId> sortKeys = sortKeysPerPartition.get(partitionKey);
			if (sortKeys == null) {
				sortKeys = new ArrayList<>();
				sortKeysPerPartition.put(
						partitionKey,
						sortKeys);
			}
			sortKeys.add(new ByteArrayId(
					key.getSortKey()));
		}
		final Set<SinglePartitionInsertionIds> insertionIds = new HashSet<>();
		for (final Entry<ByteArrayId, List<ByteArrayId>> e : sortKeysPerPartition.entrySet()) {
			insertionIds.add(new SinglePartitionInsertionIds(
					e.getKey(),
					e.getValue()));
		}
		return new InsertionIds(
				insertionIds);
	}

	public static boolean rowIdsMatch(
			final GeoWaveKey rowId1,
			final GeoWaveKey rowId2 ) {
		if (!Arrays.equals(
				rowId1.getPartitionKey(),
				rowId2.getPartitionKey())) {
			return false;
		}
		if (!Arrays.equals(
				rowId1.getSortKey(),
				rowId2.getSortKey())) {
			return false;
		}
		if (rowId1.getInternalAdapterId() != rowId2.getInternalAdapterId()) {
			return false;
		}

		if (Arrays.equals(
				rowId1.getDataId(),
				rowId2.getDataId())) {
			return true;
		}

		return Arrays.equals(
				rowId1.getDataId(),
				rowId2.getDataId());
	}

	public static byte[] removeUniqueId(
			byte[] dataId ) {
		if ((dataId.length < UNIQUE_ADDED_BYTES) || (dataId[dataId.length - UNIQUE_ADDED_BYTES] != UNIQUE_ID_DELIMITER)) {
			return dataId;
		}

		dataId = Arrays.copyOfRange(
				dataId,
				0,
				dataId.length - UNIQUE_ADDED_BYTES);

		return dataId;
	}

	/**
	 *
	 * Takes a byte array representing a serialized composite group of
	 * FieldInfos sharing a common visibility and returns a List of the
	 * individual FieldInfos
	 *
	 * @param compositeFieldId
	 *            the composite bitmask representing the fields contained within
	 *            the flattenedValue
	 * @param flattenedValue
	 *            the serialized composite FieldInfo
	 * @param commonVisibility
	 *            the shared visibility
	 * @param maxFieldPosition
	 *            can short-circuit read and defer decomposition of fields after
	 *            a given position
	 * @return the dataset that has been read
	 */
	public static <T> FlattenedDataSet decomposeFlattenedFields(
			final byte[] bitmask,
			final byte[] flattenedValue,
			final byte[] commonVisibility,
			final int maxFieldPosition ) {
		final List<FlattenedFieldInfo> fieldInfoList = new LinkedList<FlattenedFieldInfo>();
		final List<Integer> fieldPositions = BitmaskUtils.getFieldPositions(bitmask);

		final boolean sharedVisibility = fieldPositions.size() > 1;
		if (sharedVisibility) {
			final ByteBuffer input = ByteBuffer.wrap(flattenedValue);
			for (int i = 0; i < fieldPositions.size(); i++) {
				final Integer fieldPosition = fieldPositions.get(i);
				if ((maxFieldPosition > -1) && (fieldPosition > maxFieldPosition)) {
					return new FlattenedDataSet(
							fieldInfoList,
							new FlattenedUnreadDataSingleRow(
									input,
									i,
									fieldPositions));
				}
				final int fieldLength = input.getInt();
				final byte[] fieldValueBytes = new byte[fieldLength];
				input.get(fieldValueBytes);
				fieldInfoList.add(new FlattenedFieldInfo(
						fieldPosition,
						fieldValueBytes));
			}
		}
		else {
			fieldInfoList.add(new FlattenedFieldInfo(
					fieldPositions.get(0),
					flattenedValue));

		}
		return new FlattenedDataSet(
				fieldInfoList,
				null);
	}

	public static QueryRanges constraintsToQueryRanges(
			final List<MultiDimensionalNumericData> constraints,
			final NumericIndexStrategy indexStrategy,
			final int maxRanges,
			final IndexMetaData... hints ) {
		if ((constraints == null) || constraints.isEmpty()) {
			return new QueryRanges(); // implies in negative and
			// positive infinity
		}
		else {
			final List<QueryRanges> ranges = new ArrayList<>(
					constraints.size());
			for (final MultiDimensionalNumericData nd : constraints) {
				ranges.add(indexStrategy.getQueryRanges(
						nd,
						maxRanges,
						hints));
			}
			return ranges.size() > 1 ? new QueryRanges(
					ranges) : ranges.get(0);
		}
	}

	public static String getQualifiedTableName(
			final String tableNamespace,
			final String unqualifiedTableName ) {
		return ((tableNamespace == null) || tableNamespace.isEmpty()) ? unqualifiedTableName : tableNamespace + "_"
				+ unqualifiedTableName;
	}

	public static ByteArrayId ensureUniqueId(
			final byte[] id,
			final boolean hasMetadata ) {

		final ByteBuffer buf = ByteBuffer.allocate(id.length + UNIQUE_ADDED_BYTES);

		byte[] metadata = null;
		byte[] dataId;
		if (hasMetadata) {
			final int metadataStartIdx = id.length - 12;
			final byte[] lengths = Arrays.copyOfRange(
					id,
					metadataStartIdx,
					id.length);

			final ByteBuffer lengthsBuf = ByteBuffer.wrap(lengths);
			final int adapterIdLength = lengthsBuf.getInt();
			int dataIdLength = lengthsBuf.getInt();
			dataIdLength += UNIQUE_ADDED_BYTES;
			final int duplicates = lengthsBuf.getInt();

			final ByteBuffer newLengths = ByteBuffer.allocate(12);
			newLengths.putInt(adapterIdLength);
			newLengths.putInt(dataIdLength);
			newLengths.putInt(duplicates);
			newLengths.rewind();
			metadata = newLengths.array();
			dataId = Arrays.copyOfRange(
					id,
					0,
					metadataStartIdx);
		}
		else {
			dataId = id;
		}

		buf.put(dataId);

		final long timestamp = System.currentTimeMillis();
		buf.put(new byte[] {
			UNIQUE_ID_DELIMITER
		});
		final UUID uuid = UUID.randomUUID();
		buf.putLong(timestamp);
		buf.putLong(uuid.getLeastSignificantBits());
		buf.putLong(uuid.getMostSignificantBits());
		if (hasMetadata) {
			buf.put(metadata);
		}

		return new ByteArrayId(
				buf.array());
	}

	private static final byte[] BEG_AND_BYTE = "&".getBytes(StringUtils.getGeoWaveCharset());
	private static final byte[] END_AND_BYTE = ")".getBytes(StringUtils.getGeoWaveCharset());

	public static byte[] mergeVisibilities(
			final byte vis1[],
			final byte vis2[] ) {
		if ((vis1 == null) || (vis1.length == 0)) {
			return vis2;
		}
		else if ((vis2 == null) || (vis2.length == 0)) {
			return vis1;
		}

		final ByteBuffer buffer = ByteBuffer.allocate(vis1.length + 3 + vis2.length);
		buffer.putChar('(');
		buffer.put(vis1);
		buffer.putChar(')');
		buffer.put(BEG_AND_BYTE);
		buffer.put(vis2);
		buffer.put(END_AND_BYTE);
		return buffer.array();
	}

	public static boolean mergeStats(
			final DataStatisticsStore statsStore,
			final InternalAdapterStore internalAdapterStore ) {
		// Get all statistics, remove all statistics, then re-add
		try (CloseableIterator<Short> it = internalAdapterStore.getInternalAdapterIds()) {
			while (it.hasNext()) {
				Short internalAdapterId = it.next();
				if (internalAdapterId != null) {
					InternalDataStatistics<?>[] statsArray;
					try (final CloseableIterator<InternalDataStatistics<?>> stats = statsStore
							.getDataStatistics(internalAdapterId)) {
						statsArray = Iterators.toArray(
								stats,
								InternalDataStatistics.class);
					}
					catch (IOException e) {
						// wrap in a parameter exception
						throw new ParameterException(
								"Unable to combine stats",
								e);
					}
					// Clear all existing stats
					statsStore.removeAllStatistics(internalAdapterId);
					for (InternalDataStatistics<?> stats : statsArray) {
						statsStore.incorporateStatistics(stats);
					}
				}
			}
		}
		catch (IOException e) {
			LOGGER.error(
					"Cannot merge stats on table '" + AbstractGeoWavePersistence.METADATA_TABLE + "'",
					e);
			return false;
		}
		return true;
	}

	public static boolean mergeData(
			final Index index,
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore ) {
		return false;
	}
}
