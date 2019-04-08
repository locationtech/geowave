/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.Type;
import org.locationtech.geowave.core.store.entities.*;
import java.nio.ByteBuffer;
import java.util.List;

public class KuduRow extends MergeableGeoWaveRow implements PersistentKuduRow {

  private final byte[] partitionKey;
  private final short adapterId;
  private final byte[] sortKey;
  private final byte[] dataId;
  private final byte[] fieldVisibility;
  private final byte[] nanoTime;
  private final byte[] fieldMask;
  private final byte[] value;
  private final int numDuplicates;

  public enum KuduField {
    GW_PARTITION_ID_KEY("partition", Type.BINARY, KuduColumnType.PARTITION_KEY, true),
    GW_ADAPTER_ID_KEY("adapter_id", Type.INT16, KuduColumnType.CLUSTER_COLUMN, true),
    GW_SORT_KEY("sort", Type.BINARY, KuduColumnType.CLUSTER_COLUMN),
    GW_DATA_ID_KEY("data_id", Type.BINARY, KuduColumnType.CLUSTER_COLUMN),
    GW_FIELD_VISIBILITY_KEY("vis", Type.BINARY, KuduColumnType.CLUSTER_COLUMN),
    GW_NANO_TIME_KEY("nano_time", Type.BINARY, KuduColumnType.CLUSTER_COLUMN),
    GW_FIELD_MASK_KEY("field_mask", Type.BINARY, KuduColumnType.OTHER_COLUMN),
    GW_VALUE_KEY("value", Type.BINARY, KuduColumnType.OTHER_COLUMN, true),
    GW_NUM_DUPLICATES_KEY("num_duplicates", Type.INT8, KuduColumnType.OTHER_COLUMN);

    private final String fieldName;
    private final Type dataType;
    private KuduColumnType columnType;
    private final boolean isDataIndexColumn;

    KuduField(final String fieldName, final Type dataType, final KuduColumnType columnType) {
      this(fieldName, dataType, columnType, false);
    }

    KuduField(
        final String fieldName,
        final Type dataType,
        final KuduColumnType columnType,
        final boolean isDataIndexColumn) {
      this.fieldName = fieldName;
      this.dataType = dataType;
      this.columnType = columnType;
      this.isDataIndexColumn = isDataIndexColumn;
    }

    public boolean isDataIndexColumn() {
      return isDataIndexColumn;
    }

    public String getFieldName() {
      return fieldName;
    }

    public void addColumn(final List<ColumnSchema> columns) {
      columnType.createFunction.accept(columns, Pair.of(fieldName, dataType));
    }
  }

  public KuduRow(final RowResult row) {
    super(getFieldValues(row));
    this.partitionKey = row.getBinaryCopy(KuduField.GW_PARTITION_ID_KEY.getFieldName());
    this.adapterId = row.getShort(KuduField.GW_ADAPTER_ID_KEY.getFieldName());
    this.sortKey = row.getBinaryCopy(KuduField.GW_SORT_KEY.getFieldName());
    this.dataId = row.getBinaryCopy(KuduField.GW_DATA_ID_KEY.getFieldName());
    this.fieldVisibility = row.getBinaryCopy(KuduField.GW_FIELD_VISIBILITY_KEY.getFieldName());
    this.nanoTime = row.getBinaryCopy(KuduField.GW_NANO_TIME_KEY.getFieldName());
    this.fieldMask = row.getBinaryCopy(KuduField.GW_FIELD_MASK_KEY.getFieldName());
    this.value = row.getBinaryCopy(KuduField.GW_VALUE_KEY.getFieldName());
    this.numDuplicates = row.getByte(KuduField.GW_NUM_DUPLICATES_KEY.getFieldName());
  }

  public KuduRow(GeoWaveRow row, GeoWaveValue value) {
    ByteBuffer nanoBuffer = ByteBuffer.allocate(8);
    nanoBuffer.putLong(0, Long.MAX_VALUE - System.nanoTime());
    this.partitionKey = row.getPartitionKey();
    this.adapterId = row.getAdapterId();
    this.sortKey = row.getSortKey();
    this.dataId = row.getDataId();
    this.numDuplicates = row.getNumberOfDuplicates();
    this.nanoTime = nanoBuffer.array();
    this.fieldVisibility = value.getVisibility();
    this.fieldMask = value.getFieldMask();
    this.value = value.getValue();
  }

  @Override
  public byte[] getDataId() {
    return dataId;
  }

  @Override
  public byte[] getSortKey() {
    return sortKey;
  }

  @Override
  public byte[] getPartitionKey() {
    return partitionKey;
  }

  @Override
  public int getNumberOfDuplicates() {
    return numDuplicates;
  }

  @Override
  public short getAdapterId() {
    return adapterId;
  }

  private static GeoWaveValue[] getFieldValues(final RowResult row) {
    final byte[] fieldMask = row.getBinaryCopy(KuduField.GW_FIELD_MASK_KEY.getFieldName());
    final byte[] value = row.getBinaryCopy(KuduField.GW_VALUE_KEY.getFieldName());
    final byte[] visibility = row.getBinaryCopy(KuduField.GW_FIELD_VISIBILITY_KEY.getFieldName());

    return new GeoWaveValueImpl[] {new GeoWaveValueImpl(fieldMask, visibility, value)};
  }

  @Override
  public void populatePartialRow(PartialRow partialRow) {
    populatePartialRowPrimaryKey(partialRow);
    partialRow.addBinary(KuduField.GW_FIELD_MASK_KEY.getFieldName(), fieldMask);
    partialRow.addBinary(KuduField.GW_VALUE_KEY.getFieldName(), value);
    partialRow.addByte(KuduField.GW_NUM_DUPLICATES_KEY.getFieldName(), (byte) numDuplicates);
  }

  @Override
  public void populatePartialRowPrimaryKey(PartialRow partialRow) {
    partialRow.addBinary(KuduField.GW_PARTITION_ID_KEY.getFieldName(), partitionKey);
    partialRow.addShort(KuduField.GW_ADAPTER_ID_KEY.getFieldName(), adapterId);
    partialRow.addBinary(KuduField.GW_SORT_KEY.getFieldName(), sortKey);
    partialRow.addBinary(KuduField.GW_DATA_ID_KEY.getFieldName(), dataId);
    partialRow.addBinary(KuduField.GW_FIELD_VISIBILITY_KEY.getFieldName(), fieldVisibility);
    partialRow.addBinary(KuduField.GW_NANO_TIME_KEY.getFieldName(), nanoTime);
  }
}
