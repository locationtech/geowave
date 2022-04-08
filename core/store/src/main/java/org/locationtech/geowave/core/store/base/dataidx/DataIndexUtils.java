/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base.dataidx;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.operations.DataIndexReaderParams;
import org.locationtech.geowave.core.store.operations.DataIndexReaderParamsBuilder;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.util.NativeEntryIteratorWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.primitives.Bytes;

public class DataIndexUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataIndexUtils.class);
  public static final Index DATA_ID_INDEX = new NullIndex("DATA");

  public static boolean isDataIndex(final String indexName) {
    return DATA_ID_INDEX.getName().equals(indexName);
  }

  public static GeoWaveValue deserializeDataIndexValue(
      final byte[] serializedValue,
      final byte[] visibility) {
    return deserializeDataIndexValue(serializedValue, visibility, false);
  }

  public static GeoWaveValue deserializeDataIndexValue(
      final byte[] serializedValue,
      final boolean visibilityEnabled) {
    return deserializeDataIndexValue(serializedValue, null, visibilityEnabled);
  }

  public static GeoWaveValue deserializeDataIndexValue(
      final byte[] serializedValue,
      final byte[] visibilityInput,
      final boolean visibilityEnabled) {
    final ByteBuffer buf = ByteBuffer.wrap(serializedValue);
    int lengthBytes = 1;
    final byte[] fieldMask = new byte[serializedValue[serializedValue.length - 1]];
    buf.get(fieldMask);

    final byte[] visibility;
    if (visibilityInput != null) {
      visibility = visibilityInput;
    } else if (visibilityEnabled) {
      lengthBytes++;
      visibility = new byte[serializedValue[serializedValue.length - 2]];
      buf.get(visibility);
    } else {
      visibility = new byte[0];
    }
    final byte[] value = new byte[buf.remaining() - lengthBytes];
    buf.get(value);
    return new GeoWaveValueImpl(fieldMask, visibility, value);
  }

  public static boolean adapterSupportsDataIndex(final DataTypeAdapter<?> adapter) {
    // currently row merging is not supported by the data index
    return !BaseDataStoreUtils.isRowMerging(adapter);
  }

  public static GeoWaveRow deserializeDataIndexRow(
      final byte[] dataId,
      final short adapterId,
      final byte[] serializedValue,
      final byte[] serializedVisibility) {
    return new GeoWaveRowImpl(
        new GeoWaveKeyImpl(dataId, adapterId, new byte[0], new byte[0], 0),
        new GeoWaveValue[] {deserializeDataIndexValue(serializedValue, serializedVisibility)});
  }

  public static GeoWaveRow deserializeDataIndexRow(
      final byte[] dataId,
      final short adapterId,
      final byte[] serializedValue,
      final boolean visibilityEnabled) {
    return new GeoWaveRowImpl(
        new GeoWaveKeyImpl(dataId, adapterId, new byte[0], new byte[0], 0),
        new GeoWaveValue[] {deserializeDataIndexValue(serializedValue, visibilityEnabled)});
  }

  public static byte[] serializeDataIndexValue(
      final GeoWaveValue value,
      final boolean visibilityEnabled) {
    if (visibilityEnabled) {
      return Bytes.concat(
          value.getFieldMask(),
          value.getVisibility(),
          value.getValue(),
          new byte[] {(byte) value.getVisibility().length, (byte) value.getFieldMask().length});

    } else {
      return Bytes.concat(
          value.getFieldMask(),
          value.getValue(),
          new byte[] {(byte) value.getFieldMask().length});
    }
  }

  public static DataIndexRetrieval getDataIndexRetrieval(
      final DataStoreOperations operations,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final Index index,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final String[] additionalAuthorizations,
      final int dataIndexBatchSize) {
    if ((dataIndexBatchSize > 0) && !isDataIndex(index.getName())) {
      // this implies that this index merely contains a reference by data ID and a second lookup
      // must be done
      if (dataIndexBatchSize > 1) {
        return new BatchIndexRetrievalImpl(
            operations,
            adapterStore,
            mappingStore,
            internalAdapterStore,
            fieldSubsets,
            aggregation,
            additionalAuthorizations,
            dataIndexBatchSize);
      }
      return new DataIndexRetrievalImpl(
          operations,
          adapterStore,
          mappingStore,
          internalAdapterStore,
          fieldSubsets,
          aggregation,
          additionalAuthorizations);
    }
    return null;
  }

  protected static GeoWaveValue[] getFieldValuesFromDataIdIndex(
      final DataStoreOperations operations,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final String[] additionalAuthorizations,
      final Short adapterId,
      final byte[] dataId) {
    try (final RowReader<GeoWaveRow> reader =
        getRowReader(
            operations,
            adapterStore,
            mappingStore,
            internalAdapterStore,
            fieldSubsets,
            aggregation,
            additionalAuthorizations,
            adapterId,
            dataId)) {
      if (reader.hasNext()) {
        return reader.next().getFieldValues();
      } else {
        LOGGER.warn(
            "Unable to find data ID '"
                + StringUtils.stringFromBinary(dataId)
                + " (hex:"
                + ByteArrayUtils.getHexString(dataId)
                + ")' with adapter ID "
                + adapterId
                + " in data table");
      }
    } catch (final Exception e) {
      LOGGER.warn("Unable to close reader", e);
    }
    return null;
  }

  public static void delete(
      final DataStoreOperations operations,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final String[] additionalAuthorizations,
      final ScanCallback scanCallback,
      final short adapterId,
      final byte[]... dataIds) {
    final DataIndexReaderParams readerParams =
        new DataIndexReaderParamsBuilder<>(
            adapterStore,
            mappingStore,
            internalAdapterStore).additionalAuthorizations(
                additionalAuthorizations).isAuthorizationsLimiting(false).adapterId(
                    adapterId).dataIds(dataIds).fieldSubsets(fieldSubsets).aggregation(
                        aggregation).build();
    if (scanCallback != null) {
      // we need to read first to support scan callbacks and then delete (we might consider changing
      // the interface on base operations delete with DataIndexReaderParams to allow for a scan
      // callback but for now we can explicitly read before deleting)
      try (RowReader<GeoWaveRow> rowReader = operations.createReader(readerParams)) {
        final NativeEntryIteratorWrapper scanCallBackIterator =
            new NativeEntryIteratorWrapper(
                adapterStore,
                mappingStore,
                DataIndexUtils.DATA_ID_INDEX,
                rowReader,
                null,
                scanCallback,
                BaseDataStoreUtils.getFieldBitmask(fieldSubsets, DataIndexUtils.DATA_ID_INDEX),
                null,
                !BaseDataStoreUtils.isCommonIndexAggregation(aggregation),
                null);
        // just drain the iterator so the scan callback is properly exercised
        scanCallBackIterator.forEachRemaining(it -> {
        });
      }
    }
    operations.delete(readerParams);
  }

  public static void delete(
      final DataStoreOperations operations,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final String[] additionalAuthorizations,
      final ScanCallback<?, ?> scanCallback,
      final short adapterId,
      final byte[] startDataId,
      final byte[] endDataId) {
    // TODO within the datastores delete by range is not supported (the deletion logic expect Data
    // IDs to be non-null within reader params and deletions don't have logic for handling ranges

    // for now, although less efficient, let's query by prefix and then delete by the returned IDs

    final DataIndexReaderParams readerParams =
        new DataIndexReaderParamsBuilder<>(
            adapterStore,
            mappingStore,
            internalAdapterStore).additionalAuthorizations(
                additionalAuthorizations).isAuthorizationsLimiting(false).adapterId(
                    adapterId).dataIdsByRange(startDataId, endDataId).fieldSubsets(
                        fieldSubsets).aggregation(aggregation).build();
    final List<byte[]> dataIds = new ArrayList<>();
    try (RowReader<GeoWaveRow> reader = operations.createReader(readerParams)) {
      while (reader.hasNext()) {
        dataIds.add(reader.next().getDataId());
      }
    }
    delete(
        operations,
        adapterStore,
        mappingStore,
        internalAdapterStore,
        fieldSubsets,
        aggregation,
        additionalAuthorizations,
        scanCallback,
        adapterId,
        dataIds.toArray(new byte[dataIds.size()][]));
  }

  public static RowReader<GeoWaveRow> getRowReader(
      final DataStoreOperations operations,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final String[] additionalAuthorizations,
      final short adapterId,
      final byte[]... dataIds) {
    final DataIndexReaderParams readerParams =
        new DataIndexReaderParamsBuilder<>(
            adapterStore,
            mappingStore,
            internalAdapterStore).additionalAuthorizations(
                additionalAuthorizations).isAuthorizationsLimiting(false).adapterId(
                    adapterId).dataIds(dataIds).fieldSubsets(fieldSubsets).aggregation(
                        aggregation).build();
    return operations.createReader(readerParams);
  }

  public static RowReader<GeoWaveRow> getRowReader(
      final DataStoreOperations operations,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final String[] additionalAuthorizations,
      final short adapterId,
      final byte[] startDataId,
      final byte[] endDataId,
      final boolean reverse) {
    final DataIndexReaderParams readerParams =
        new DataIndexReaderParamsBuilder<>(
            adapterStore,
            mappingStore,
            internalAdapterStore).additionalAuthorizations(
                additionalAuthorizations).isAuthorizationsLimiting(false).adapterId(
                    adapterId).dataIdsByRange(startDataId, endDataId, reverse).fieldSubsets(
                        fieldSubsets).aggregation(aggregation).build();
    return operations.createReader(readerParams);
  }

  public static RowReader<GeoWaveRow> getRowReader(
      final DataStoreOperations operations,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final String[] additionalAuthorizations,
      final short adapterId) {
    final DataIndexReaderParams readerParams =
        new DataIndexReaderParamsBuilder<>(
            adapterStore,
            mappingStore,
            internalAdapterStore).additionalAuthorizations(
                additionalAuthorizations).isAuthorizationsLimiting(false).adapterId(
                    adapterId).fieldSubsets(fieldSubsets).aggregation(aggregation).build();
    return operations.createReader(readerParams);
  }
}
