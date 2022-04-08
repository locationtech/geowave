/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.CustomIndexStrategy;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import org.locationtech.geowave.core.index.IndexUtils;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIterator.Wrapper;
import org.locationtech.geowave.core.store.DataStoreProperty;
import org.locationtech.geowave.core.store.PropertyStore;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.AdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.AsyncPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.FullAsyncPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.LazyReadPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.PartialAsyncPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.adapter.exceptions.AdapterException;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.AttributeIndex;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;
import org.locationtech.geowave.core.store.api.IndexFieldMapper.IndexFieldOptions;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.base.IntermediaryWriteEntryInfo.FieldInfo;
import org.locationtech.geowave.core.store.base.dataidx.BatchDataIndexRetrieval;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexRetrieval;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.data.DataWriter;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.data.visibility.VisibilityComposer;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.flatten.BitmaskUtils;
import org.locationtech.geowave.core.store.flatten.BitmaskedPairComparator;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.IndexFieldMapperRegistry;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.query.aggregate.CommonIndexAggregation;
import org.locationtech.geowave.core.store.query.constraints.AdapterAndIndexBasedQueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.core.store.statistics.DefaultStatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.internal.Maps;
import com.google.common.base.Suppliers;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class BaseDataStoreUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseDataStoreUtils.class);

  public static final String DATA_VERSION_PROPERTY = "DATA_VERSION";
  public static final String GLOBAL_VISIBILITY_PROPERTY = "GLOBAL_VISIBILITY";
  public static final Integer DATA_VERSION = 1;

  public static void verifyCLIVersion(
      final String storeName,
      final DataStorePluginOptions options) {
    final DataStoreOperations operations = options.createDataStoreOperations();
    final PropertyStore propertyStore = options.createPropertyStore();
    final DataStoreProperty storeVersionProperty = propertyStore.getProperty(DATA_VERSION_PROPERTY);
    if ((storeVersionProperty == null)
        && !hasMetadata(operations, MetadataType.ADAPTER)
        && !hasMetadata(operations, MetadataType.INDEX)) {
      // Nothing has been loaded into the store yet
      return;
    }
    final int storeVersion =
        storeVersionProperty == null ? 0 : (int) storeVersionProperty.getValue();
    if (storeVersion < DATA_VERSION) {
      throw new ParameterException(
          "The data store '"
              + storeName
              + "' is using an older serialization format.  Either use an older "
              + "version of the CLI that is compatible with the data store, or migrate the data "
              + "store to a later version using the `geowave util migrate` command.");
    } else if (storeVersion > DATA_VERSION) {
      throw new ParameterException(
          "The data store '"
              + storeName
              + "' is using a newer serialization format.  Please update to a "
              + "newer version of the CLI that is compatible with the data store.");
    }
  }

  public static boolean hasMetadata(
      final DataStoreOperations operations,
      final MetadataType metadataType) {
    try {
      if (!operations.metadataExists(metadataType)) {
        return false;
      }
    } catch (final IOException e) {
      LOGGER.warn("Error while checking existence of metadata table", e);
    }
    final MetadataReader reader = operations.createMetadataReader(metadataType);
    try (CloseableIterator<GeoWaveMetadata> it = reader.query(new MetadataQuery(null))) {
      return it.hasNext();
    }
  }

  public static <T> GeoWaveRow[] getGeoWaveRows(
      final T entry,
      final InternalDataAdapter<T> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index,
      final VisibilityHandler visibilityHandler) {
    return getWriteInfo(
        entry,
        adapter,
        indexMapping,
        index,
        visibilityHandler,
        false,
        false,
        true).getRows();
  }

  public static CloseableIterator<Object> aggregate(
      final CloseableIterator<Object> it,
      final Aggregation<?, ?, Object> aggregationFunction,
      final DataTypeAdapter<Object> adapter) {
    if ((it != null) && it.hasNext()) {
      try {
        synchronized (aggregationFunction) {
          aggregationFunction.clearResult();
          while (it.hasNext()) {
            final Object input = it.next();
            if (input != null) {
              aggregationFunction.aggregate(adapter, input);
            }
          }
        }
      } finally {
        it.close();
      }
      return new Wrapper<>(Iterators.singletonIterator(aggregationFunction.getResult()));
    }
    return new CloseableIterator.Empty<>();
  }

  /**
   * Basic method that decodes a native row Currently overridden by Accumulo and HBase; Unification
   * in progress
   *
   * <p> Override this method if you can't pass in a GeoWaveRow!
   *
   * @throws AdapterException
   */
  public static <T> Object decodeRow(
      final GeoWaveRow geowaveRow,
      final QueryFilter[] clientFilters,
      final InternalDataAdapter<T> adapter,
      final AdapterToIndexMapping indexMapping,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final Index index,
      final ScanCallback scanCallback,
      final byte[] fieldSubsetBitmask,
      final boolean decodeRow,
      final DataIndexRetrieval dataIndexRetrieval) throws AdapterException {
    final short internalAdapterId = geowaveRow.getAdapterId();

    if ((adapter == null) && (adapterStore == null)) {
      final String msg =
          "Could not decode row from iterator. Either adapter or adapter store must be non-null.";
      LOGGER.error(msg);
      throw new AdapterException(msg);
    }
    final IntermediaryReadEntryInfo decodePackage = new IntermediaryReadEntryInfo(index, decodeRow);

    if (!decodePackage.setOrRetrieveAdapter(adapter, internalAdapterId, adapterStore)) {
      final String msg = "Could not retrieve adapter " + internalAdapterId + " from adapter store.";
      LOGGER.error(msg);
      throw new AdapterException(msg);
    }

    if (!decodePackage.setOrRetrieveIndexMapping(indexMapping, internalAdapterId, mappingStore)) {
      final String msg =
          "Could not retrieve adapter index mapping for adapter " + internalAdapterId;
      LOGGER.error(msg);
      throw new AdapterException(msg);
    }

    // Verify the adapter matches the data
    if (!decodePackage.isAdapterVerified()) {
      if (!decodePackage.verifyAdapter(internalAdapterId)) {
        final String msg = "Adapter verify failed: adapter does not match data.";
        LOGGER.error(msg);
        throw new AdapterException(msg);
      }
    }

    return getDecodedRow(
        geowaveRow,
        decodePackage,
        fieldSubsetBitmask,
        clientFilters,
        scanCallback,
        decodePackage.adapterSupportsDataIndex() ? dataIndexRetrieval : null);
  }

  /**
   * build a persistence encoding object first, pass it through the client filters and if its
   * accepted, use the data adapter to decode the persistence model into the native data type
   */
  private static <T> Object getDecodedRow(
      final GeoWaveRow row,
      final IntermediaryReadEntryInfo<T> decodePackage,
      final byte[] fieldSubsetBitmask,
      final QueryFilter[] clientFilters,
      final ScanCallback<T, GeoWaveRow> scanCallback,
      final DataIndexRetrieval dataIndexRetrieval) {
    final boolean isSecondaryIndex = dataIndexRetrieval != null;
    final IndexedAdapterPersistenceEncoding encodedRow;
    if (isSecondaryIndex) {
      // this implies its a Secondary Index and the actual values must be looked up
      if (dataIndexRetrieval instanceof BatchDataIndexRetrieval) {
        if (decodePackage.getIndex().getIndexModel().useInSecondaryIndex()) {
          encodedRow =
              new PartialAsyncPersistenceEncoding(
                  decodePackage.getDataAdapter().getAdapterId(),
                  row.getDataId(),
                  row.getPartitionKey(),
                  row.getSortKey(),
                  row.getNumberOfDuplicates(),
                  (BatchDataIndexRetrieval) dataIndexRetrieval,
                  decodePackage.getDataAdapter(),
                  decodePackage.getIndex().getIndexModel(),
                  decodePackage.getIndexMapping(),
                  fieldSubsetBitmask,
                  Suppliers.memoize(
                      () -> dataIndexRetrieval.getData(
                          decodePackage.getDataAdapter().getAdapterId(),
                          row.getDataId())));
        } else {
          encodedRow =
              new FullAsyncPersistenceEncoding(
                  decodePackage.getDataAdapter().getAdapterId(),
                  row.getDataId(),
                  row.getPartitionKey(),
                  row.getSortKey(),
                  row.getNumberOfDuplicates(),
                  (BatchDataIndexRetrieval) dataIndexRetrieval);
        }
      } else {
        encodedRow =
            new LazyReadPersistenceEncoding(
                decodePackage.getDataAdapter().getAdapterId(),
                row.getDataId(),
                row.getPartitionKey(),
                row.getSortKey(),
                row.getNumberOfDuplicates(),
                decodePackage.getDataAdapter(),
                decodePackage.getIndex().getIndexModel(),
                decodePackage.getIndexMapping(),
                fieldSubsetBitmask,
                Suppliers.memoize(
                    () -> dataIndexRetrieval.getData(
                        decodePackage.getDataAdapter().getAdapterId(),
                        row.getDataId())));
      }
    } else {
      encodedRow =
          new LazyReadPersistenceEncoding(
              decodePackage.getDataAdapter().getAdapterId(),
              row.getDataId(),
              row.getPartitionKey(),
              row.getSortKey(),
              row.getNumberOfDuplicates(),
              decodePackage.getDataAdapter(),
              decodePackage.getIndex().getIndexModel(),
              decodePackage.getIndexMapping(),
              fieldSubsetBitmask,
              row.getFieldValues(),
              false);
    }
    final BiFunction<IndexedAdapterPersistenceEncoding, Integer, Object> function =
        ((r, initialFilter) -> {
          final int i =
              clientFilterProgress(
                  clientFilters,
                  decodePackage.getIndex().getIndexModel(),
                  r,
                  initialFilter);
          if (i < 0) {
            if (!decodePackage.isDecodeRow()) {
              return r;
            }
            final T decodedRow =
                decodePackage.getDataAdapter().decode(
                    r,
                    decodePackage.getIndexMapping(),
                    isSecondaryIndex ? DataIndexUtils.DATA_ID_INDEX : decodePackage.getIndex());
            if (r.isAsync()) {
              return i;
            }
            if ((scanCallback != null)) {
              scanCallback.entryScanned(decodedRow, row);
            }

            return decodedRow;
          }
          if (r.isAsync()) {
            return i;
          }
          return null;
        });
    final Object obj = function.apply(encodedRow, 0);
    if ((obj instanceof Integer) && encodedRow.isAsync()) {
      // by re-applying the function, client filters should not be called multiple times for the
      // same instance (beware of stateful filters such as dedupe filter). this method attempts to
      // maintain progress of the filter chain so that any successful filters prior to retrieving
      // the data will not need to be repeated
      return (((AsyncPersistenceEncoding) encodedRow).getFieldValuesFuture().thenApply(
          fv -> new LazyReadPersistenceEncoding(
              decodePackage.getDataAdapter().getAdapterId(),
              row.getDataId(),
              row.getPartitionKey(),
              row.getSortKey(),
              row.getNumberOfDuplicates(),
              decodePackage.getDataAdapter(),
              decodePackage.getIndex().getIndexModel(),
              decodePackage.getIndexMapping(),
              fieldSubsetBitmask,
              fv,
              true))).thenApply((r) -> function.apply(r, (Integer) obj));
    }
    return obj;
  }

  /**
   *
   * @return returns -1 if all client filters have accepted the row, otherwise returns how many
   *         client filters have accepted
   */
  private static int clientFilterProgress(
      final QueryFilter[] clientFilters,
      final CommonIndexModel indexModel,
      final IndexedAdapterPersistenceEncoding encodedRow,
      final int initialFilter) {
    if ((clientFilters == null) || (initialFilter < 0)) {
      return -1;
    }
    for (int i = initialFilter; i < clientFilters.length; i++) {
      if (!clientFilters[i].accept(indexModel, encodedRow)) {
        return i;
      }
    }
    return -1;
  }

  public static byte[] getFieldBitmask(
      final Pair<String[], InternalDataAdapter<?>> fieldIdsAdapterPair,
      final Index index) {
    if ((fieldIdsAdapterPair != null) && (fieldIdsAdapterPair.getLeft() != null)) {
      return BitmaskUtils.generateFieldSubsetBitmask(
          index.getIndexModel(),
          fieldIdsAdapterPair.getLeft(),
          fieldIdsAdapterPair.getRight());
    }
    return null;
  }

  private static <T> void addIndexFieldVisibility(
      final T entry,
      final DataTypeAdapter<T> adapter,
      final AdapterToIndexMapping indexMapping,
      final VisibilityHandler visibilityHandler,
      final String indexField,
      final VisibilityComposer baseVisibility) {
    final String[] adapterFields =
        indexMapping.getMapperForIndexField(indexField).getAdapterFields();
    for (final String adapterField : adapterFields) {
      final String adapterFieldVisibility =
          visibilityHandler.getVisibility(adapter, entry, adapterField);
      baseVisibility.addVisibility(adapterFieldVisibility);
    }
  }

  protected static <T> IntermediaryWriteEntryInfo getWriteInfo(
      final T entry,
      final InternalDataAdapter<T> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index,
      final VisibilityHandler visibilityHandler,
      final boolean secondaryIndex,
      final boolean dataIdIndex,
      final boolean visibilityEnabled) {
    final CommonIndexModel indexModel = index.getIndexModel();
    final short internalAdapterId = adapter.getAdapterId();
    final byte[] dataId = adapter.getDataId(entry);
    final AdapterPersistenceEncoding encodedData = adapter.encode(entry, indexMapping, index);
    if (encodedData == null) {
      // The entry could not be encoded to the index, but this could be due to a null value in one
      // of the index fields, which is possible in attribute indices
      LOGGER.info(
          "Indexing failed to produce insertion ids; entry ["
              + StringUtils.stringFromBinary(adapter.getDataId(entry))
              + "] not saved for index '"
              + index.getName()
              + "'.");

      return new IntermediaryWriteEntryInfo(
          dataId,
          internalAdapterId,
          new InsertionIds(),
          new GeoWaveValueImpl[0]);
    }
    final InsertionIds insertionIds;
    if (index instanceof CustomIndexStrategy) {
      insertionIds = ((CustomIndexStrategy) index).getInsertionIds(entry);
    } else {
      insertionIds = dataIdIndex ? null : encodedData.getInsertionIds(index);
    }

    if (dataIdIndex) {
      return getWriteInfoDataIDIndex(
          entry,
          dataId,
          encodedData,
          adapter,
          indexMapping,
          index,
          visibilityHandler,
          visibilityEnabled);
    }
    if (insertionIds.isEmpty()) {
      // we can allow some entries to not be indexed within every index for flexibility, and
      // therefore this should just be info level
      LOGGER.info(
          "Indexing failed to produce insertion ids; entry ["
              + StringUtils.stringFromBinary(adapter.getDataId(entry))
              + "] not saved for index '"
              + index.getName()
              + "'.");

      return new IntermediaryWriteEntryInfo(
          dataId,
          internalAdapterId,
          insertionIds,
          new GeoWaveValueImpl[0]);
    }

    final VisibilityComposer commonIndexVisibility = new VisibilityComposer();
    if (visibilityEnabled && (visibilityHandler != null)) {
      for (final Entry<String, Object> fieldValue : encodedData.getCommonData().getValues().entrySet()) {
        addIndexFieldVisibility(
            entry,
            adapter,
            indexMapping,
            visibilityHandler,
            fieldValue.getKey(),
            commonIndexVisibility);
      }
    }
    if (secondaryIndex && DataIndexUtils.adapterSupportsDataIndex(adapter)) {
      return new IntermediaryWriteEntryInfo(
          dataId,
          internalAdapterId,
          insertionIds,
          new GeoWaveValue[] {
              new GeoWaveValueImpl(
                  new byte[0],
                  StringUtils.stringToBinary(commonIndexVisibility.composeVisibility()),
                  new byte[0])});
    }
    final List<FieldInfo<?>> fieldInfoList = new ArrayList<>();
    addCommonFields(
        adapter,
        indexMapping,
        entry,
        index,
        indexModel,
        visibilityHandler,
        encodedData,
        visibilityEnabled,
        fieldInfoList);
    for (final Entry<String, Object> fieldValue : encodedData.getAdapterExtendedData().getValues().entrySet()) {
      if (fieldValue.getValue() != null) {
        final FieldInfo<?> fieldInfo =
            getFieldInfo(
                adapter,
                adapter,
                indexMapping,
                fieldValue.getKey(),
                fieldValue.getValue(),
                entry,
                visibilityHandler,
                visibilityEnabled,
                false);
        if (fieldInfo != null) {
          fieldInfoList.add(fieldInfo);
        }
      }
    }

    return new IntermediaryWriteEntryInfo(
        dataId,
        internalAdapterId,
        insertionIds,
        BaseDataStoreUtils.composeFlattenedFields(
            fieldInfoList,
            indexModel,
            adapter,
            commonIndexVisibility,
            dataIdIndex));
  }

  protected static <T> IntermediaryWriteEntryInfo getWriteInfoDataIDIndex(
      final T entry,
      final byte[] dataId,
      final AdapterPersistenceEncoding encodedData,
      final InternalDataAdapter<T> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index,
      final VisibilityHandler visibilityHandler,
      final boolean visibilityEnabled) {
    final List<FieldInfo<?>> fieldInfoList = new ArrayList<>();
    addCommonFields(
        adapter,
        indexMapping,
        entry,
        index,
        index.getIndexModel(),
        visibilityHandler,
        encodedData,
        visibilityEnabled,
        fieldInfoList);
    for (final Entry<String, Object> fieldValue : encodedData.getAdapterExtendedData().getValues().entrySet()) {
      if (fieldValue.getValue() != null) {
        final FieldInfo<?> fieldInfo =
            getFieldInfo(
                adapter,
                adapter,
                indexMapping,
                fieldValue.getKey(),
                fieldValue.getValue(),
                entry,
                visibilityHandler,
                visibilityEnabled,
                false);
        if (fieldInfo != null) {
          fieldInfoList.add(fieldInfo);
        }
      }
    }

    return new IntermediaryWriteEntryInfo(
        dataId,
        adapter.getAdapterId(),
        null,
        BaseDataStoreUtils.composeFlattenedFields(
            fieldInfoList,
            index.getIndexModel(),
            adapter,
            null,
            true));
  }

  private static <T> void addCommonFields(
      final DataTypeAdapter<T> adapter,
      final AdapterToIndexMapping indexMapping,
      final T entry,
      final Index index,
      final CommonIndexModel indexModel,
      final VisibilityHandler visibilityHandler,
      final AdapterPersistenceEncoding encodedData,
      final boolean visibilityEnabled,
      final List<FieldInfo<?>> fieldInfoList) {

    for (final Entry<String, Object> fieldValue : encodedData.getCommonData().getValues().entrySet()) {
      final FieldInfo<?> fieldInfo =
          getFieldInfo(
              indexModel,
              adapter,
              indexMapping,
              fieldValue.getKey(),
              fieldValue.getValue(),
              entry,
              visibilityHandler,
              visibilityEnabled,
              true);
      if (fieldInfo != null) {
        fieldInfoList.add(fieldInfo);
      }
    }
  }

  /**
   * This method combines all FieldInfos that share a common visibility into a single FieldInfo
   *
   * @param originalList
   * @return a new list of composite FieldInfos
   */
  private static <T> GeoWaveValue[] composeFlattenedFields(
      final List<FieldInfo<?>> originalList,
      final CommonIndexModel model,
      final InternalDataAdapter<?> writableAdapter,
      final VisibilityComposer commonIndexVisibility,
      final boolean dataIdIndex) {
    if (originalList.isEmpty()) {
      return new GeoWaveValue[0];
    }
    final Map<String, List<Pair<Integer, FieldInfo<?>>>> vizToFieldMap = new LinkedHashMap<>();
    // organize FieldInfos by unique visibility
    if (dataIdIndex) {
      final List<Pair<Integer, FieldInfo<?>>> fieldsWithPositions =
          (List) originalList.stream().map(fieldInfo -> {
            final int fieldPosition =
                writableAdapter.getPositionOfOrderedField(model, fieldInfo.getFieldId());
            return (Pair) Pair.of(fieldPosition, fieldInfo);
          }).collect(Collectors.toList());
      final VisibilityComposer combinedVisibility = new VisibilityComposer();
      for (final FieldInfo<?> fieldValue : originalList) {
        combinedVisibility.addVisibility(fieldValue.getVisibility());
      }
      vizToFieldMap.put(combinedVisibility.composeVisibility(), fieldsWithPositions);
    } else {
      boolean sharedVisibility = false;
      for (final FieldInfo<?> fieldInfo : originalList) {
        int fieldPosition =
            writableAdapter.getPositionOfOrderedField(model, fieldInfo.getFieldId());
        if (fieldPosition == -1) {
          // this is just a fallback for unexpected failures
          fieldPosition = writableAdapter.getPositionOfOrderedField(model, fieldInfo.getFieldId());
        }
        final VisibilityComposer currentComposer = new VisibilityComposer(commonIndexVisibility);
        currentComposer.addVisibility(fieldInfo.getVisibility());
        final String currViz = currentComposer.composeVisibility();
        if (vizToFieldMap.containsKey(currViz)) {
          sharedVisibility = true;
          final List<Pair<Integer, FieldInfo<?>>> listForViz = vizToFieldMap.get(currViz);
          listForViz.add(new ImmutablePair<Integer, FieldInfo<?>>(fieldPosition, fieldInfo));
        } else {
          final List<Pair<Integer, FieldInfo<?>>> listForViz = new LinkedList<>();
          listForViz.add(new ImmutablePair<Integer, FieldInfo<?>>(fieldPosition, fieldInfo));
          vizToFieldMap.put(currViz, listForViz);
        }
      }

      if (!sharedVisibility) {
        // at a minimum, must return transformed (bitmasked) fieldInfos
        final GeoWaveValue[] bitmaskedValues = new GeoWaveValue[vizToFieldMap.size()];
        int i = 0;
        for (final List<Pair<Integer, FieldInfo<?>>> list : vizToFieldMap.values()) {
          // every list must have exactly one element
          final Pair<Integer, FieldInfo<?>> fieldInfo = list.get(0);
          bitmaskedValues[i++] =
              new GeoWaveValueImpl(
                  BitmaskUtils.generateCompositeBitmask(fieldInfo.getLeft()),
                  StringUtils.stringToBinary(fieldInfo.getRight().getVisibility()),
                  fieldInfo.getRight().getWrittenValue());
        }
        return bitmaskedValues;
      }
    }
    if (vizToFieldMap.size() == 1) {
      return new GeoWaveValue[] {entryToValue(vizToFieldMap.entrySet().iterator().next())};
    } else {
      final List<GeoWaveValue> retVal = new ArrayList<>(vizToFieldMap.size());
      for (final Entry<String, List<Pair<Integer, FieldInfo<?>>>> entry : vizToFieldMap.entrySet()) {
        retVal.add(entryToValue(entry));
      }
      return retVal.toArray(new GeoWaveValue[0]);
    }
  }

  private static GeoWaveValue entryToValue(
      final Entry<String, List<Pair<Integer, FieldInfo<?>>>> entry) {
    final SortedSet<Integer> fieldPositions = new TreeSet<>();
    final List<Pair<Integer, FieldInfo<?>>> fieldInfoList = entry.getValue();
    final byte[] combinedValue =
        fieldInfoList.size() > 1 ? combineValues(fieldInfoList)
            : fieldInfoList.size() > 0 ? fieldInfoList.get(0).getRight().getWrittenValue()
                : new byte[0];
    fieldInfoList.stream().forEach(p -> fieldPositions.add(p.getLeft()));
    final byte[] compositeBitmask = BitmaskUtils.generateCompositeBitmask(fieldPositions);
    return new GeoWaveValueImpl(
        compositeBitmask,
        StringUtils.stringToBinary(entry.getKey()),
        combinedValue);
  }

  private static byte[] combineValues(final List<Pair<Integer, FieldInfo<?>>> fieldInfoList) {
    int totalLength = 0;
    Collections.sort(fieldInfoList, new BitmaskedPairComparator());
    final List<byte[]> fieldInfoBytesList = new ArrayList<>(fieldInfoList.size());
    for (final Pair<Integer, FieldInfo<?>> fieldInfoPair : fieldInfoList) {
      final FieldInfo<?> fieldInfo = fieldInfoPair.getRight();
      final ByteBuffer fieldInfoBytes =
          ByteBuffer.allocate(
              VarintUtils.unsignedIntByteLength(fieldInfo.getWrittenValue().length)
                  + fieldInfo.getWrittenValue().length);
      VarintUtils.writeUnsignedInt(fieldInfo.getWrittenValue().length, fieldInfoBytes);
      fieldInfoBytes.put(fieldInfo.getWrittenValue());
      fieldInfoBytesList.add(fieldInfoBytes.array());
      totalLength += fieldInfoBytes.array().length;
    }
    final ByteBuffer allFields = ByteBuffer.allocate(totalLength);
    for (final byte[] bytes : fieldInfoBytesList) {
      allFields.put(bytes);
    }
    return allFields.array();
  }

  private static <T> FieldInfo<?> getFieldInfo(
      final DataWriter dataWriter,
      final DataTypeAdapter<T> adapter,
      final AdapterToIndexMapping indexMapping,
      final String fieldName,
      final Object fieldValue,
      final T entry,
      final VisibilityHandler visibilityHandler,
      final boolean visibilityEnabled,
      final boolean indexField) {
    final FieldWriter fieldWriter = dataWriter.getWriter(fieldName);
    if (fieldWriter != null) {
      final VisibilityComposer visibilityComposer = new VisibilityComposer();
      if (visibilityEnabled && (visibilityHandler != null)) {
        if (indexField) {
          addIndexFieldVisibility(
              entry,
              adapter,
              indexMapping,
              visibilityHandler,
              fieldName,
              visibilityComposer);
        } else {
          visibilityComposer.addVisibility(
              visibilityHandler.getVisibility(adapter, entry, fieldName));
        }
      }
      return new FieldInfo(
          fieldName,
          fieldWriter.writeField(fieldValue),
          visibilityComposer.composeVisibility());
    } else if (fieldValue != null) {
      LOGGER.warn(
          "Data writer of class "
              + dataWriter.getClass()
              + " does not support field for "
              + fieldValue);
    }
    return null;
  }

  private static <T> void sortInPlace(final List<Pair<Index, T>> input) {
    Collections.sort(input, new Comparator<Pair<Index, T>>() {

      @Override
      public int compare(final Pair<Index, T> o1, final Pair<Index, T> o2) {
        if (o1.getKey() == null) {
          if (o2.getKey() == null) {
            return 0;
          }
          return 1;
        }
        if (o2.getKey() == null) {
          return -1;
        }
        return o1.getKey().getName().compareTo(o2.getKey().getName());
      }
    });
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static AdapterToIndexMapping mapAdapterToIndex(
      final InternalDataAdapter<?> adapter,
      final Index index) {
    // Build up a list of index field mappers
    final Map<String, IndexFieldMapper<?, ?>> mappers = Maps.newHashMap();

    // Get index model dimensions
    final NumericDimensionField<?>[] dimensions = index.getIndexModel().getDimensions();

    // Map dimensions to index fields
    final Map<String, List<NumericDimensionField>> indexFields =
        Arrays.stream(dimensions).collect(
            Collectors.groupingBy(
                dim -> dim.getFieldName(),
                Collectors.mapping(dim -> dim, Collectors.toList())));

    // Get adapter fields
    final FieldDescriptor<?>[] adapterFields = adapter.getFieldDescriptors();

    for (final Entry<String, List<NumericDimensionField>> indexField : indexFields.entrySet()) {
      // Get the hints used by all dimensions of the field
      final Set<IndexDimensionHint> dimensionHints = Sets.newHashSet();
      indexField.getValue().forEach(dim -> dimensionHints.addAll(dim.getDimensionHints()));

      final Class<?> indexFieldClass = indexField.getValue().get(0).getFieldClass();
      final String indexFieldName = indexField.getKey();
      final IndexFieldOptions indexFieldOptions =
          indexField.getValue().get(0).getIndexFieldOptions();

      // Get available mappers for the field class
      final List<IndexFieldMapper<?, ?>> availableMappers =
          IndexFieldMapperRegistry.instance().getAvailableMappers(indexFieldClass);
      if (availableMappers.size() == 0) {
        throw new IllegalArgumentException(
            "No index field mappers were found for the type: " + indexFieldClass.getName());
      }

      final List<FieldDescriptor<?>> hintedFields;
      if (index instanceof AttributeIndex) {
        // Only check the field that is set for the attribute index
        hintedFields =
            Lists.newArrayList(
                adapter.getFieldDescriptor(((AttributeIndex) index).getAttributeName()));
      } else {
        // Get any adapter fields that have been hinted for this field
        hintedFields =
            Arrays.stream(adapterFields).filter(
                field -> dimensionHints.stream().anyMatch(field.indexHints()::contains)).collect(
                    Collectors.toList());
      }

      if (hintedFields.size() > 0) {
        final Class<?> hintedFieldClass = hintedFields.get(0).bindingClass();
        for (int i = 1; i < hintedFields.size(); i++) {
          if (!hintedFieldClass.equals(hintedFields.get(i).bindingClass())) {
            throw new IllegalArgumentException("All hinted fields must be of the same type.");
          }
        }
        boolean mapperFound = false;
        // Find a mapper that matches
        for (final IndexFieldMapper<?, ?> mapper : availableMappers) {
          if (mapper.isCompatibleWith(hintedFieldClass)
              && (mapper.adapterFieldCount() == hintedFields.size())) {
            mapper.init(indexField.getKey(), (List) hintedFields, indexFieldOptions);
            mappers.put(indexField.getKey(), mapper);
            mapperFound = true;
            break;
          }
        }
        if (!mapperFound) {
          throw new IllegalArgumentException(
              "No registered index field mappers were found for the type: "
                  + hintedFieldClass.getName()
                  + "["
                  + hintedFields.size()
                  + "] -> "
                  + indexFieldClass.getName());
        }
      } else {
        // Attempt to infer the field to use

        // See if there are any suggested fields
        boolean mapperFound = false;
        for (final IndexFieldMapper<?, ?> mapper : availableMappers) {
          final Set<String> suggestedFieldNames = mapper.getLowerCaseSuggestedFieldNames();
          final List<FieldDescriptor<?>> matchingFields =
              Arrays.stream(adapterFields).filter(
                  field -> mapper.isCompatibleWith(field.bindingClass())
                      && suggestedFieldNames.contains(field.fieldName().toLowerCase())).collect(
                          Collectors.toList());
          if (matchingFields.size() >= mapper.adapterFieldCount()) {
            mapperFound = true;
            mapper.init(
                indexFieldName,
                (List) matchingFields.stream().limit(mapper.adapterFieldCount()).collect(
                    Collectors.toList()),
                indexFieldOptions);
            mappers.put(indexFieldName, mapper);
            break;
          }
        }

        // See if a direct mapper is available
        if (!mapperFound) {
          for (final FieldDescriptor<?> fieldDescriptor : adapterFields) {
            if (fieldDescriptor.bindingClass().equals(indexFieldClass)) {
              final Optional<IndexFieldMapper<?, ?>> matchingMapper =
                  availableMappers.stream().filter(
                      mapper -> mapper.isCompatibleWith(fieldDescriptor.bindingClass())
                          && (mapper.adapterFieldCount() == 1)).findFirst();
              if (matchingMapper.isPresent()) {
                final IndexFieldMapper<?, ?> mapper = matchingMapper.get();
                mapperFound = true;
                mapper.init(
                    indexFieldName,
                    (List) Lists.newArrayList(fieldDescriptor),
                    indexFieldOptions);
                mappers.put(indexFieldName, mapper);
                break;
              }
            }
          }
        }

        // Check other mappers
        if (!mapperFound) {
          for (final IndexFieldMapper<?, ?> mapper : availableMappers) {
            final List<FieldDescriptor<?>> matchingFields =
                Arrays.stream(adapterFields).filter(
                    field -> mapper.isCompatibleWith(field.bindingClass())).collect(
                        Collectors.toList());
            if (matchingFields.size() >= mapper.adapterFieldCount()) {
              mapperFound = true;
              mapper.init(
                  indexFieldName,
                  (List) matchingFields.stream().limit(mapper.adapterFieldCount()).collect(
                      Collectors.toList()),
                  indexFieldOptions);
              mappers.put(indexFieldName, mapper);
              break;
            }
          }
        }

        if (!mapperFound) {
          throw new IllegalArgumentException(
              "No suitable index field mapper could be found for the index field "
                  + indexFieldName);
        }
      }
    }
    return new AdapterToIndexMapping(
        adapter.getAdapterId(),
        index.getName(),
        mappers.values().stream().collect(Collectors.toList()));
  }

  public static <T> List<Pair<Index, List<T>>> combineByIndex(final List<Pair<Index, T>> input) {
    final List<Pair<Index, List<T>>> result = new ArrayList<>();
    sortInPlace(input);
    List<T> valueSet = new ArrayList<>();
    Pair<Index, T> last = null;
    for (final Pair<Index, T> item : input) {
      if ((last != null)
          && (item.getKey() != null)
          && ((last.getKey() == null)
              || !last.getKey().getName().equals(item.getKey().getName()))) {
        result.add(Pair.of(last.getLeft(), valueSet));
        valueSet = new ArrayList<>();
      }
      valueSet.add(item.getValue());
      last = item;
    }
    if (last != null) {
      result.add(Pair.of(last.getLeft(), valueSet));
    }
    return result;
  }

  public static List<Pair<Index, List<InternalDataAdapter<?>>>> chooseBestIndex(
      final List<Pair<Index, List<InternalDataAdapter<?>>>> indexAdapterPairList,
      final QueryConstraints query,
      final AdapterIndexMappingStore mappingStore) {
    return chooseBestIndex(indexAdapterPairList, mappingStore, query, Function.identity());
  }

  public static <T> List<Pair<Index, List<T>>> chooseBestIndex(
      final List<Pair<Index, List<T>>> indexAdapterPairList,
      final AdapterIndexMappingStore mappingStore,
      final QueryConstraints query,
      final Function<T, ? extends InternalDataAdapter<?>> adapterLookup)
      throws IllegalArgumentException {
    if (indexAdapterPairList.size() <= 1) {
      return indexAdapterPairList;
    }
    if ((query != null) && query.indexMustBeSpecified()) {
      throw new IllegalArgumentException("Query constraint requires specifying exactly one index");
    }
    final Map<T, List<Index>> indicesPerAdapter = new HashMap<>();
    for (final Pair<Index, List<T>> pair : indexAdapterPairList) {
      for (final T adapter : pair.getRight()) {
        List<Index> indices = indicesPerAdapter.get(adapter);
        if (indices == null) {
          indices = new ArrayList<>();
          indicesPerAdapter.put(adapter, indices);
        }
        indices.add(pair.getLeft());
      }
    }
    final Map<Index, List<T>> retVal = new HashMap<>();
    for (final Entry<T, List<Index>> e : indicesPerAdapter.entrySet()) {
      final Index index =
          query == null ? e.getValue().get(0)
              : chooseBestIndex(
                  e.getValue().toArray(new Index[0]),
                  query,
                  adapterLookup.apply(e.getKey()),
                  mappingStore);
      List<T> adapters = retVal.get(index);
      if (adapters == null) {
        adapters = new ArrayList<>();
        retVal.put(index, adapters);
      }
      adapters.add(e.getKey());
    }
    return retVal.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).collect(
        Collectors.toList());
  }

  public static Index chooseBestIndex(
      final Index[] indices,
      final QueryConstraints query,
      final InternalDataAdapter<?> adapter,
      final AdapterIndexMappingStore mappingStore) {
    final boolean isConstraintsAdapterIndexSpecific =
        query instanceof AdapterAndIndexBasedQueryConstraints;
    Index nextIdx = null;
    int i = 0;

    double bestIndexBitsUsed = -1;
    int bestIndexDimensionCount = -1;
    Index bestIdx = null;
    while (i < indices.length) {
      nextIdx = indices[i++];
      if ((nextIdx == null)
          || (nextIdx.getIndexStrategy() == null)
          || (nextIdx.getIndexStrategy().getOrderedDimensionDefinitions() == null)
          || (nextIdx.getIndexStrategy().getOrderedDimensionDefinitions().length == 0)) {
        continue;
      }

      QueryConstraints adapterIndexConstraints;
      if (isConstraintsAdapterIndexSpecific) {
        adapterIndexConstraints =
            ((AdapterAndIndexBasedQueryConstraints) query).createQueryConstraints(
                adapter,
                nextIdx,
                mappingStore.getMapping(adapter.getAdapterId(), nextIdx.getName()));
        if (adapterIndexConstraints == null) {
          continue;
        }
      } else {
        adapterIndexConstraints = query;
      }
      final List<MultiDimensionalNumericData> queryRanges =
          adapterIndexConstraints.getIndexConstraints(nextIdx);
      final int currentDimensionCount =
          nextIdx.getIndexStrategy().getOrderedDimensionDefinitions().length;
      if (IndexUtils.isFullTableScan(queryRanges)
          || !queryRangeDimensionsMatch(currentDimensionCount, queryRanges)) {
        // keep this is as a default in case all indices
        // result in a full table scan
        if (bestIdx == null) {
          bestIdx = nextIdx;
        }
      } else {
        double currentBitsUsed = 0;

        if (currentDimensionCount >= bestIndexDimensionCount) {
          for (final MultiDimensionalNumericData qr : queryRanges) {
            final double[] dataRangePerDimension = new double[qr.getDimensionCount()];
            for (int d = 0; d < dataRangePerDimension.length; d++) {
              dataRangePerDimension[d] =
                  qr.getMaxValuesPerDimension()[d] - qr.getMinValuesPerDimension()[d];
            }
            currentBitsUsed +=
                IndexUtils.getDimensionalBitsUsed(
                    nextIdx.getIndexStrategy(),
                    dataRangePerDimension);
          }

          if ((currentDimensionCount > bestIndexDimensionCount)
              || (currentBitsUsed > bestIndexBitsUsed)) {
            bestIndexBitsUsed = currentBitsUsed;
            bestIndexDimensionCount = currentDimensionCount;
            bestIdx = nextIdx;
          }
        }
      }
    }
    if ((bestIdx == null) && (indices.length > 0)) {
      bestIdx = indices[0];
    }
    return bestIdx;
  }

  private static boolean queryRangeDimensionsMatch(
      final int indexDimensions,
      final List<MultiDimensionalNumericData> queryRanges) {
    for (final MultiDimensionalNumericData qr : queryRanges) {
      if (qr.getDimensionCount() != indexDimensions) {
        return false;
      }
    }
    return true;
  }

  public static List<Pair<Index, List<Short>>> getAdaptersWithMinimalSetOfIndices(
      final @Nullable String[] typeNames,
      final @Nullable String indexName,
      final TransientAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final AdapterIndexMappingStore adapterIndexMappingStore,
      final IndexStore indexStore,
      final QueryConstraints constraints) throws IOException {
    return chooseBestIndex(
        reduceIndicesAndGroupByIndex(
            compileIndicesForAdapters(
                typeNames,
                indexName,
                adapterStore,
                internalAdapterStore,
                adapterIndexMappingStore,
                indexStore)),
        adapterIndexMappingStore,
        constraints,
        adapterId -> {
          final String typeName = internalAdapterStore.getTypeName(adapterId);
          if (typeName != null) {
            final DataTypeAdapter<?> adapter = adapterStore.getAdapter(typeName);
            if (adapter != null) {
              return adapter.asInternalAdapter(adapterId);
            }
          }
          return null;
        });
  }

  private static List<Pair<Index, Short>> compileIndicesForAdapters(
      final @Nullable String[] typeNames,
      final @Nullable String indexName,
      final TransientAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final AdapterIndexMappingStore adapterIndexMappingStore,
      final IndexStore indexStore) throws IOException {
    Collection<Short> adapterIds;
    if ((typeNames == null) || (typeNames.length == 0)) {
      adapterIds = Arrays.asList(ArrayUtils.toObject(internalAdapterStore.getAdapterIds()));
    } else {
      adapterIds =
          Collections2.filter(
              Lists.transform(
                  Arrays.asList(typeNames),
                  typeName -> internalAdapterStore.getAdapterId(typeName)),
              adapterId -> adapterId != null);
    }
    final List<Pair<Index, Short>> result = new ArrayList<>();
    for (final Short adapterId : adapterIds) {
      final AdapterToIndexMapping[] indices =
          adapterIndexMappingStore.getIndicesForAdapter(adapterId);
      if ((indexName != null)
          && Arrays.stream(indices).anyMatch(mapping -> mapping.getIndexName().equals(indexName))) {
        result.add(Pair.of(indexStore.getIndex(indexName), adapterId));
      } else if (indices.length > 0) {
        for (final AdapterToIndexMapping mapping : indices) {
          final Index pIndex = mapping.getIndex(indexStore);
          // this could happen if persistent was turned off
          if (pIndex != null) {
            result.add(Pair.of(pIndex, adapterId));
          }
        }
      }
    }
    return result;
  }

  protected static <T> List<Pair<Index, List<T>>> reduceIndicesAndGroupByIndex(
      final List<Pair<Index, T>> input) {
    final Map<Index, List<T>> result = Maps.newHashMap();
    input.forEach(pair -> {
      if (!result.containsKey(pair.getLeft())) {
        result.put(pair.getLeft(), Lists.newArrayList());
      }
      result.get(pair.getLeft()).add(pair.getRight());
    });
    return result.entrySet().stream().map(
        entry -> Pair.of(entry.getKey(), entry.getValue())).collect(Collectors.toList());
  }

  public static DefaultStatisticsProvider getDefaultStatisticsProvider(
      final DataTypeAdapter<?> adapter) {
    if (adapter instanceof InternalDataAdapter) {
      return getDefaultStatisticsProvider(((InternalDataAdapter) adapter).getAdapter());
    }
    return adapter instanceof DefaultStatisticsProvider ? (DefaultStatisticsProvider) adapter
        : null;
  }

  public static RowMergingDataAdapter<?, ?> getRowMergingAdapter(final DataTypeAdapter<?> adapter) {
    if (adapter instanceof InternalDataAdapter) {
      return getRowMergingAdapter(((InternalDataAdapter) adapter).getAdapter());
    }
    return adapter instanceof RowMergingDataAdapter ? (RowMergingDataAdapter) adapter : null;
  }

  public static boolean isRowMerging(final DataTypeAdapter<?> adapter) {
    if (adapter instanceof InternalDataAdapter) {
      return isRowMerging(((InternalDataAdapter) adapter).getAdapter());
    }
    return (adapter instanceof RowMergingDataAdapter)
        && (((RowMergingDataAdapter) adapter).getTransform() != null);
  }

  public static boolean isRowMerging(
      final PersistentAdapterStore adapterStore,
      final short[] adapterIds) {
    if (adapterIds != null) {
      for (final short adapterId : adapterIds) {
        if (isRowMerging(adapterStore.getAdapter(adapterId).getAdapter())) {
          return true;
        }
      }
    } else {
      final InternalDataAdapter<?>[] adapters = adapterStore.getAdapters();
      for (final InternalDataAdapter<?> adapter : adapters) {
        if (isRowMerging(adapter.getAdapter())) {
          return true;
        }
      }
    }
    return false;
  }

  public static boolean isAggregation(
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation) {
    return (aggregation != null) && (aggregation.getRight() != null);
  }

  public static boolean isCommonIndexAggregation(
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation) {
    return isAggregation(aggregation) && (aggregation.getRight() instanceof CommonIndexAggregation);
  }
}
