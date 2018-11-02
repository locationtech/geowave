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
package org.locationtech.geowave.core.store.base;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.annotation.Nullable;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIterator.Wrapper;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.AdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.AdapterStore;
import org.locationtech.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.adapter.exceptions.AdapterException;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.IntermediaryWriteEntryInfo.FieldInfo;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.data.DataWriter;
import org.locationtech.geowave.core.store.data.VisibilityWriter;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.flatten.BitmaskUtils;
import org.locationtech.geowave.core.store.flatten.BitmaskedPairComparator;
import org.locationtech.geowave.core.store.flatten.FlattenedFieldInfo;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

public class BaseDataStoreUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BaseDataStoreUtils.class);

	public static <T> GeoWaveRow[] getGeoWaveRows(
			final T entry,
			final InternalDataAdapter<T> adapter,
			final Index index,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		return getWriteInfo(
				entry,
				adapter,
				index,
				customFieldVisibilityWriter).getRows();
	}

	/**
	 * Basic method that decodes a native row Currently overridden by Accumulo
	 * and HBase; Unification in progress
	 *
	 * Override this method if you can't pass in a GeoWaveRow!
	 *
	 * @throws AdapterException
	 */
	public static <T> Object decodeRow(
			final GeoWaveRow geowaveRow,
			final QueryFilter clientFilter,
			final InternalDataAdapter<T> adapter,
			final PersistentAdapterStore adapterStore,
			final Index index,
			final ScanCallback scanCallback,
			final byte[] fieldSubsetBitmask,
			final boolean decodeRow )
			throws AdapterException {
		final short internalAdapterId = geowaveRow.getAdapterId();

		if ((adapter == null) && (adapterStore == null)) {
			final String msg = "Could not decode row from iterator. Either adapter or adapter store must be non-null.";
			LOGGER.error(msg);
			throw new AdapterException(
					msg);
		}
		final IntermediaryReadEntryInfo decodePackage = new IntermediaryReadEntryInfo(
				index,
				decodeRow);

		if (!decodePackage.setOrRetrieveAdapter(
				adapter,
				internalAdapterId,
				adapterStore)) {
			final String msg = "Could not retrieve adapter " + internalAdapterId + " from adapter store.";
			LOGGER.error(msg);
			throw new AdapterException(
					msg);
		}

		// Verify the adapter matches the data
		if (!decodePackage.isAdapterVerified()) {
			if (!decodePackage.verifyAdapter(internalAdapterId)) {
				final String msg = "Adapter verify failed: adapter does not match data.";
				LOGGER.error(msg);
				throw new AdapterException(
						msg);
			}
		}

		for (final GeoWaveValue value : geowaveRow.getFieldValues()) {
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

			readValue(
					decodePackage,
					new GeoWaveValueImpl(
							fieldMask,
							value.getVisibility(),
							byteValue));
		}

		return getDecodedRow(
				geowaveRow,
				decodePackage,
				clientFilter,
				scanCallback);
	}

	public static CloseableIterator<Object> aggregate(
			final CloseableIterator<Object> it,
			final Aggregation<?, ?, Object> aggregationFunction ) {
		if ((it != null) && it.hasNext()) {
			try {
				synchronized (aggregationFunction) {
					aggregationFunction.clearResult();
					while (it.hasNext()) {
						final Object input = it.next();
						if (input != null) {
							aggregationFunction.aggregate(input);
						}
					}
				}
			}
			finally {
				it.close();
			}
			return new Wrapper(
					Iterators.singletonIterator(aggregationFunction.getResult()));
		}
		return new CloseableIterator.Empty();
	}

	/**
	 * build a persistence encoding object first, pass it through the client
	 * filters and if its accepted, use the data adapter to decode the
	 * persistence model into the native data type
	 */
	private static <T> Object getDecodedRow(
			final GeoWaveRow row,
			final IntermediaryReadEntryInfo<T> decodePackage,
			final QueryFilter clientFilter,
			final ScanCallback<T, GeoWaveRow> scanCallback ) {
		final IndexedAdapterPersistenceEncoding encodedRow = new IndexedAdapterPersistenceEncoding(
				decodePackage.getDataAdapter().getAdapterId(),
				new ByteArray(
						row.getDataId()),
				new ByteArray(
						row.getPartitionKey()),
				new ByteArray(
						row.getSortKey()),
				row.getNumberOfDuplicates(),
				decodePackage.getIndexData(),
				decodePackage.getUnknownData(),
				decodePackage.getExtendedData());

		if ((clientFilter == null) || clientFilter.accept(
				decodePackage.getIndex().getIndexModel(),
				encodedRow)) {
			if (!decodePackage.isDecodeRow()) {
				return encodedRow;
			}

			final T decodedRow = decodePackage.getDataAdapter().decode(
					encodedRow,
					decodePackage.getIndex());

			if (scanCallback != null) {
				scanCallback.entryScanned(
						decodedRow,
						row);
			}

			return decodedRow;
		}

		return null;
	}

	/**
	 * Generic field reader - updates fieldInfoList from field input data
	 */
	private static void readValue(
			final IntermediaryReadEntryInfo decodePackage,
			final GeoWaveValue value ) {
		final List<FlattenedFieldInfo> fieldInfos = DataStoreUtils.decomposeFlattenedFields(
				value.getFieldMask(),
				value.getValue(),
				value.getVisibility(),
				-1).getFieldsRead();
		for (final FlattenedFieldInfo fieldInfo : fieldInfos) {
			final String fieldName = decodePackage.getDataAdapter().getFieldNameForPosition(
					decodePackage.getIndex().getIndexModel(),
					fieldInfo.getFieldPosition());
			final FieldReader<? extends CommonIndexValue> indexFieldReader = decodePackage
					.getIndex()
					.getIndexModel()
					.getReader(
							fieldName);
			if (indexFieldReader != null) {
				final CommonIndexValue indexValue = indexFieldReader.readField(fieldInfo.getValue());
				indexValue.setVisibility(value.getVisibility());
				decodePackage.getIndexData().addValue(
						fieldName,
						indexValue);
			}
			else {
				final FieldReader<?> extFieldReader = decodePackage.getDataAdapter().getReader(
						fieldName);
				if (extFieldReader != null) {
					final Object objValue = extFieldReader.readField(fieldInfo.getValue());
					// TODO GEOWAVE-1018, do we care about visibility
					decodePackage.getExtendedData().addValue(
							fieldName,
							objValue);
				}
				else {
					LOGGER.error("field reader not found for data entry, the value may be ignored");
					decodePackage.getUnknownData().addValue(
							fieldName,
							fieldInfo.getValue());
				}
			}
		}
	}

	protected static <T> IntermediaryWriteEntryInfo getWriteInfo(
			final T entry,
			final InternalDataAdapter<T> adapter,
			final Index index,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final CommonIndexModel indexModel = index.getIndexModel();

		final AdapterPersistenceEncoding encodedData = adapter.encode(
				entry,
				indexModel);
		final InsertionIds insertionIds = encodedData.getInsertionIds(index);

		final List<FieldInfo<?>> fieldInfoList = new ArrayList<>();

		final byte[] dataId = adapter.getDataId(
				entry).getBytes();
		final short internalAdapterId = adapter.getAdapterId();
		if (!insertionIds.isEmpty()) {
			for (final Entry<String, CommonIndexValue> fieldValue : encodedData.getCommonData().getValues().entrySet()) {
				final FieldInfo<?> fieldInfo = getFieldInfo(
						indexModel,
						fieldValue.getKey(),
						fieldValue.getValue(),
						entry,
						customFieldVisibilityWriter);
				if (fieldInfo != null) {
					fieldInfoList.add(fieldInfo);
				}
			}
			for (final Entry<String, Object> fieldValue : encodedData.getAdapterExtendedData().getValues().entrySet()) {
				if (fieldValue.getValue() != null) {
					final FieldInfo<?> fieldInfo = getFieldInfo(
							adapter,
							fieldValue.getKey(),
							fieldValue.getValue(),
							entry,
							customFieldVisibilityWriter);
					if (fieldInfo != null) {
						fieldInfoList.add(fieldInfo);
					}
				}
			}
		}
		else {
			LOGGER.warn("Indexing failed to produce insertion ids; entry [" + adapter.getDataId(
					entry).getString() + "] not saved.");
		}

		return new IntermediaryWriteEntryInfo(
				dataId,
				internalAdapterId,
				insertionIds,
				BaseDataStoreUtils.composeFlattenedFields(
						fieldInfoList,
						index.getIndexModel(),
						adapter));
	}

	/**
	 * This method combines all FieldInfos that share a common visibility into a
	 * single FieldInfo
	 *
	 * @param originalList
	 * @return a new list of composite FieldInfos
	 */
	private static <T> GeoWaveValue[] composeFlattenedFields(
			final List<FieldInfo<?>> originalList,
			final CommonIndexModel model,
			final DataTypeAdapter<?> writableAdapter ) {
		final List<GeoWaveValue> retVal = new ArrayList<>();
		final Map<ByteArray, List<Pair<Integer, FieldInfo<?>>>> vizToFieldMap = new LinkedHashMap<>();
		boolean sharedVisibility = false;
		// organize FieldInfos by unique visibility
		for (final FieldInfo<?> fieldInfo : originalList) {
			int fieldPosition = writableAdapter.getPositionOfOrderedField(
					model,
					fieldInfo.getFieldId());
			if (fieldPosition == -1) {
				// this is just a fallback for unexpected failures
				fieldPosition = writableAdapter.getPositionOfOrderedField(
						model,
						fieldInfo.getFieldId());
			}
			final ByteArray currViz = new ByteArray(
					fieldInfo.getVisibility());
			if (vizToFieldMap.containsKey(currViz)) {
				sharedVisibility = true;
				final List<Pair<Integer, FieldInfo<?>>> listForViz = vizToFieldMap.get(currViz);
				listForViz.add(new ImmutablePair<Integer, FieldInfo<?>>(
						fieldPosition,
						fieldInfo));
			}
			else {
				final List<Pair<Integer, FieldInfo<?>>> listForViz = new LinkedList<>();
				listForViz.add(new ImmutablePair<Integer, FieldInfo<?>>(
						fieldPosition,
						fieldInfo));
				vizToFieldMap.put(
						currViz,
						listForViz);
			}
		}
		if (!sharedVisibility) {
			// at a minimum, must return transformed (bitmasked) fieldInfos
			final GeoWaveValue[] bitmaskedValues = new GeoWaveValue[vizToFieldMap.size()];
			int i = 0;
			for (final List<Pair<Integer, FieldInfo<?>>> list : vizToFieldMap.values()) {
				// every list must have exactly one element
				final Pair<Integer, FieldInfo<?>> fieldInfo = list.get(0);
				bitmaskedValues[i++] = new GeoWaveValueImpl(
						BitmaskUtils.generateCompositeBitmask(fieldInfo.getLeft()),
						fieldInfo.getRight().getVisibility(),
						fieldInfo.getRight().getWrittenValue());
			}
			return bitmaskedValues;
		}
		for (final Entry<ByteArray, List<Pair<Integer, FieldInfo<?>>>> entry : vizToFieldMap.entrySet()) {
			int totalLength = 0;
			final SortedSet<Integer> fieldPositions = new TreeSet<>();
			final List<Pair<Integer, FieldInfo<?>>> fieldInfoList = entry.getValue();
			Collections.sort(
					fieldInfoList,
					new BitmaskedPairComparator());
			final List<byte[]> fieldInfoBytesList = new ArrayList<>(
					fieldInfoList.size());
			for (final Pair<Integer, FieldInfo<?>> fieldInfoPair : fieldInfoList) {
				final FieldInfo<?> fieldInfo = fieldInfoPair.getRight();
				final ByteBuffer fieldInfoBytes = ByteBuffer.allocate(4 + fieldInfo.getWrittenValue().length);
				fieldPositions.add(fieldInfoPair.getLeft());
				fieldInfoBytes.putInt(fieldInfo.getWrittenValue().length);
				fieldInfoBytes.put(fieldInfo.getWrittenValue());
				fieldInfoBytesList.add(fieldInfoBytes.array());
				totalLength += fieldInfoBytes.array().length;
			}
			final ByteBuffer allFields = ByteBuffer.allocate(totalLength);
			for (final byte[] bytes : fieldInfoBytesList) {
				allFields.put(bytes);
			}
			final byte[] compositeBitmask = BitmaskUtils.generateCompositeBitmask(fieldPositions);
			retVal.add(new GeoWaveValueImpl(
					compositeBitmask,
					entry.getKey().getBytes(),
					allFields.array()));
		}
		return retVal.toArray(new GeoWaveValue[0]);
	}

	private static <T> FieldInfo<?> getFieldInfo(
			final DataWriter dataWriter,
			final String fieldName,
			final Object fieldValue,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final FieldWriter fieldWriter = dataWriter.getWriter(fieldName);
		final FieldVisibilityHandler<T, Object> customVisibilityHandler = customFieldVisibilityWriter
				.getFieldVisibilityHandler(fieldName);
		if (fieldWriter != null) {
			return new FieldInfo(
					fieldName,
					fieldWriter.writeField(fieldValue),
					DataStoreUtils.mergeVisibilities(
							customVisibilityHandler.getVisibility(
									entry,
									fieldName,
									fieldValue),
							fieldWriter.getVisibility(
									entry,
									fieldName,
									fieldValue)));
		}
		else if (fieldValue != null) {
			LOGGER.warn("Data writer of class " + dataWriter.getClass() + " does not support field for " + fieldValue);
		}
		return null;
	}

	private static <T> void sortInPlace(
			final List<Pair<Index, T>> input ) {
		Collections.sort(
				input,
				new Comparator<Pair<Index, T>>() {

					@Override
					public int compare(
							final Pair<Index, T> o1,
							final Pair<Index, T> o2 ) {

						return o1.getKey().getName().compareTo(
								o1.getKey().getName());
					}
				});
	}

	public static <T> List<Pair<Index, List<T>>> combineByIndex(
			final List<Pair<Index, T>> input ) {
		final List<Pair<Index, List<T>>> result = new ArrayList<>();
		sortInPlace(input);
		List<T> valueSet = new ArrayList<>();
		Pair<Index, T> last = null;
		for (final Pair<Index, T> item : input) {
			if ((last != null) && !last.getKey().getName().equals(
					item.getKey().getName())) {
				result.add(Pair.of(
						last.getLeft(),
						valueSet));
				valueSet = new ArrayList<>();

			}
			valueSet.add(item.getValue());
			last = item;
		}
		if (last != null) {
			result.add(Pair.of(
					last.getLeft(),
					valueSet));
		}
		return result;
	}

	public static List<Pair<Index, List<Short>>> getAdaptersWithMinimalSetOfIndices(
			final @Nullable String[] typeNames,
			final @Nullable String indexName,
			final TransientAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final IndexStore indexStore )
			throws IOException {
		return reduceIndicesAndGroupByIndex(compileIndicesForAdapters(
				typeNames,
				indexName,
				adapterStore,
				internalAdapterStore,
				adapterIndexMappingStore,
				indexStore));
	}

	private static List<Pair<Index, Short>> compileIndicesForAdapters(
			final @Nullable String[] typeNames,
			final @Nullable String indexName,
			final TransientAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final IndexStore indexStore )
			throws IOException {
		Collection<Short> adapterIds;
		if ((typeNames == null) || (typeNames.length == 0)) {
			adapterIds = Arrays
					.asList(
							ArrayUtils
									.toObject(
											internalAdapterStore.getAdapterIds()));
		}
		else {
			adapterIds = Collections2
					.filter(
							Lists
									.transform(
											Arrays
													.asList(
															typeNames),
											typeName -> internalAdapterStore
													.getAdapterId(
															typeName)),
							adapterId -> adapterId != null);
		}
		final List<Pair<Index, Short>> result = new ArrayList<>();
		for (final Short adapterId : adapterIds) {
			final AdapterToIndexMapping indices = adapterIndexMappingStore
					.getIndicesForAdapter(
							adapterId);
			if ((indexName != null) && indices
					.contains(
							indexName)) {
				result
						.add(
								Pair
										.of(
												indexStore
														.getIndex(
																indexName),
												adapterId));
			}
			else if (indices.isNotEmpty()) {
				for (final String name : indices.getIndexNames()) {
					final Index pIndex = indexStore
							.getIndex(
									name);
					// this could happen if persistent was turned off
					if (pIndex != null) {
						result
								.add(
										Pair
												.of(
														pIndex,
														adapterId));
					}
				}
			}
		}
		return result;
	}

	protected static <T> List<Pair<Index, List<T>>> reduceIndicesAndGroupByIndex(
			final List<Pair<Index, T>> input ) {
		final List<Pair<Index, T>> result = new ArrayList<>();
		// sort by index to eliminate the amount of indices returned
		sortInPlace(input);
		final Set<T> adapterSet = new HashSet<>();
		for (final Pair<Index, T> item : input) {
			if (adapterSet.add(item.getRight())) {
				result.add(item);
			}
		}
		return combineByIndex(result);
	}
}
