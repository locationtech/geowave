/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.filters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.AbstractAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.DeferredReadCommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.MultiFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.flatten.FlattenedDataSet;
import org.locationtech.geowave.core.store.flatten.FlattenedFieldInfo;
import org.locationtech.geowave.core.store.flatten.FlattenedUnreadData;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.IndexImpl;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.mapreduce.URLClassloaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class wraps our Distributable filters in an HBase filter so that a coprocessor can use them.
 *
 * @author kent
 */
public class HBaseDistributableFilter extends FilterBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(HBaseDistributableFilter.class);

  private boolean wholeRowFilter = false;
  private final List<QueryFilter> filterList;
  protected CommonIndexModel model;
  private List<String> commonIndexFieldIds = new ArrayList<>();

  // CACHED decoded data:
  private PersistentDataset<Object> commonData;
  private FlattenedUnreadData unreadData;
  private CommonIndexedPersistenceEncoding persistenceEncoding;
  private IndexedAdapterPersistenceEncoding adapterEncoding;
  private int partitionKeyLength;

  public HBaseDistributableFilter() {
    filterList = new ArrayList<>();
  }

  public static HBaseDistributableFilter parseFrom(final byte[] pbBytes)
      throws DeserializationException {
    final ByteBuffer buf = ByteBuffer.wrap(pbBytes);

    final boolean wholeRow = buf.get() == (byte) 1 ? true : false;

    final int partitionKeyLength = VarintUtils.readUnsignedInt(buf);

    final int modelLength = VarintUtils.readUnsignedInt(buf);

    final byte[] modelBytes = new byte[modelLength];
    buf.get(modelBytes);

    final byte[] filterBytes = new byte[buf.remaining()];
    buf.get(filterBytes);


    final HBaseDistributableFilter newInstance = new HBaseDistributableFilter();
    newInstance.setWholeRowFilter(wholeRow);
    newInstance.setPartitionKeyLength(partitionKeyLength);
    newInstance.init(filterBytes, modelBytes);

    return newInstance;
  }

  @Override
  public byte[] toByteArray() throws IOException {
    final byte[] modelBinary = URLClassloaderUtils.toBinary(model);
    final byte[] filterListBinary = URLClassloaderUtils.toBinary(filterList);

    final ByteBuffer buf =
        ByteBuffer.allocate(
            modelBinary.length
                + filterListBinary.length
                + 1
                + VarintUtils.unsignedIntByteLength(partitionKeyLength)
                + VarintUtils.unsignedIntByteLength(modelBinary.length));

    buf.put(wholeRowFilter ? (byte) 1 : (byte) 0);
    VarintUtils.writeUnsignedInt(partitionKeyLength, buf);
    VarintUtils.writeUnsignedInt(modelBinary.length, buf);
    buf.put(modelBinary);
    buf.put(filterListBinary);
    return buf.array();
  }

  public boolean init(final byte[] filterBytes, final byte[] modelBytes) {
    filterList.clear();
    if ((filterBytes != null) && (filterBytes.length > 0)) {
      final List<Persistable> decodedFilterList = URLClassloaderUtils.fromBinaryAsList(filterBytes);

      if (decodedFilterList == null) {
        LOGGER.error("Failed to decode filter list");
        return false;
      }

      for (final Persistable decodedFilter : decodedFilterList) {
        if (decodedFilter instanceof QueryFilter) {
          filterList.add((QueryFilter) decodedFilter);
        } else {
          LOGGER.warn("Unrecognized type for decoded filter!" + decodedFilter.getClass().getName());
        }
      }
    }

    model = (CommonIndexModel) URLClassloaderUtils.fromBinary(modelBytes);

    if (model == null) {
      LOGGER.error("Failed to decode index model");
      return false;
    }

    commonIndexFieldIds = DataStoreUtils.getUniqueDimensionFields(model);

    return true;
  }

  public boolean init(
      final List<QueryFilter> filterList,
      final CommonIndexModel model,
      final String[] visList) {
    this.filterList.clear();
    this.filterList.addAll(filterList);

    this.model = model;

    commonIndexFieldIds = DataStoreUtils.getUniqueDimensionFields(model);

    return true;
  }

  public void setWholeRowFilter(final boolean wholeRowFilter) {
    this.wholeRowFilter = wholeRowFilter;
  }

  /**
   * If true (wholeRowFilter == true), then the filter will use the filterRowCells method instead of
   * filterKeyValue
   */
  @Override
  public boolean hasFilterRow() {
    return wholeRowFilter;
  }

  /** Handle the entire row at one time */
  @Override
  public void filterRowCells(final List<Cell> rowCells) throws IOException {
    if (!rowCells.isEmpty()) {
      final Iterator<Cell> it = rowCells.iterator();

      GeoWaveKeyImpl rowKey = null;
      commonData = new MultiFieldPersistentDataset<>();

      while (it.hasNext()) {
        final Cell cell = it.next();

        // Grab rowkey from first cell
        if (rowKey == null) {
          rowKey =
              new GeoWaveKeyImpl(
                  cell.getRowArray(),
                  partitionKeyLength,
                  cell.getRowOffset(),
                  cell.getRowLength());
        }

        unreadData = aggregateFieldData(cell, commonData);
      }

      final ReturnCode code = applyFilter(rowKey);

      if (code == ReturnCode.SKIP) {
        rowCells.clear();
      }
    }
  }

  /** filterKeyValue is executed second */
  @Override
  public ReturnCode filterKeyValue(final Cell cell) throws IOException {
    if (wholeRowFilter) {
      // let filterRowCells do the work
      return ReturnCode.INCLUDE_AND_NEXT_COL;
    }

    commonData = new MultiFieldPersistentDataset<>();

    unreadData = aggregateFieldData(cell, commonData);

    return applyFilter(cell);
  }

  protected ReturnCode applyFilter(final Cell cell) {
    final GeoWaveKeyImpl rowKey =
        new GeoWaveKeyImpl(
            cell.getRowArray(),
            partitionKeyLength,
            cell.getRowOffset(),
            cell.getRowLength());

    return applyFilter(rowKey);
  }

  protected ReturnCode applyFilter(final GeoWaveKeyImpl rowKey) {
    persistenceEncoding = getPersistenceEncoding(rowKey, commonData, unreadData);
    if (filterInternal(persistenceEncoding)) {
      return ReturnCode.INCLUDE_AND_NEXT_COL;
    }

    return ReturnCode.SKIP;
  }

  protected static CommonIndexedPersistenceEncoding getPersistenceEncoding(
      final GeoWaveKeyImpl rowKey,
      final PersistentDataset<Object> commonData,
      final FlattenedUnreadData unreadData) {

    return new DeferredReadCommonIndexedPersistenceEncoding(
        rowKey.getAdapterId(),
        rowKey.getDataId(),
        rowKey.getPartitionKey(),
        rowKey.getSortKey(),
        rowKey.getNumberOfDuplicates(),
        commonData,
        unreadData);
  }

  public CommonIndexedPersistenceEncoding getPersistenceEncoding() {
    return persistenceEncoding;
  }

  public IndexedAdapterPersistenceEncoding getAdapterEncoding(
      final InternalDataAdapter<?> dataAdapter) {
    final PersistentDataset<Object> adapterExtendedValues = new MultiFieldPersistentDataset<>();
    if (persistenceEncoding instanceof AbstractAdapterPersistenceEncoding) {
      ((AbstractAdapterPersistenceEncoding) persistenceEncoding).convertUnknownValues(
          dataAdapter,
          model);
      final PersistentDataset<Object> existingExtValues =
          ((AbstractAdapterPersistenceEncoding) persistenceEncoding).getAdapterExtendedData();
      if (existingExtValues != null) {
        adapterExtendedValues.addValues(existingExtValues.getValues());
      }
    }

    adapterEncoding =
        new IndexedAdapterPersistenceEncoding(
            persistenceEncoding.getInternalAdapterId(),
            persistenceEncoding.getDataId(),
            persistenceEncoding.getInsertionPartitionKey(),
            persistenceEncoding.getInsertionSortKey(),
            persistenceEncoding.getDuplicateCount(),
            persistenceEncoding.getCommonData(),
            new MultiFieldPersistentDataset<byte[]>(),
            adapterExtendedValues);

    return adapterEncoding;
  }

  // Called by the aggregation endpoint, after filtering the current row
  public Object decodeRow(
      final InternalDataAdapter<?> dataAdapter,
      final AdapterToIndexMapping indexMapping) {
    return dataAdapter.decode(
        getAdapterEncoding(dataAdapter),
        indexMapping,
        new IndexImpl(null, model));
  }

  protected boolean filterInternal(final CommonIndexedPersistenceEncoding encoding) {
    if (filterList == null) {
      LOGGER.error("FILTER IS NULL");
      return false;
    }

    if (model == null) {
      LOGGER.error("MODEL IS NULL");
      return false;
    }

    if (encoding == null) {
      LOGGER.error("ENCODING IS NULL");
      return false;
    }

    for (final QueryFilter filter : filterList) {
      if (!filter.accept(model, encoding)) {
        return false;
      }
    }

    return true;
  }

  protected FlattenedUnreadData aggregateFieldData(
      final Cell cell,
      final PersistentDataset<Object> commonData) throws IOException {
    final byte[] qualBuf = CellUtil.cloneQualifier(cell);
    final byte[] valBuf = CellUtil.cloneValue(cell);

    final FlattenedDataSet dataSet =
        DataStoreUtils.decomposeFlattenedFields(
            qualBuf,
            valBuf,
            null,
            commonIndexFieldIds.size() - 1);

    final List<FlattenedFieldInfo> fieldInfos = dataSet.getFieldsRead();
    for (final FlattenedFieldInfo fieldInfo : fieldInfos) {
      final int ordinal = fieldInfo.getFieldPosition();

      if (ordinal < commonIndexFieldIds.size()) {
        final String commonIndexFieldName = commonIndexFieldIds.get(ordinal);
        final FieldReader<?> reader = model.getReader(commonIndexFieldName);
        if (reader != null) {
          final Object fieldValue = reader.readField(fieldInfo.getValue());
          commonData.addValue(commonIndexFieldName, fieldValue);
        } else {
          LOGGER.error("Could not find reader for common index field: " + commonIndexFieldName);
        }
      }
    }

    return dataSet.getFieldsDeferred();
  }

  public int getPartitionKeyLength() {
    return partitionKeyLength;
  }

  public void setPartitionKeyLength(final int partitionKeyLength) {
    this.partitionKeyLength = partitionKeyLength;
  }
}
