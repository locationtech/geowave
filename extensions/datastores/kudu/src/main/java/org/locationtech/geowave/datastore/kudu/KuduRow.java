/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.entities.MergeableGeoWaveRow;

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
    GW_PARTITION_ID_KEY("partition", Type.BINARY, KuduColumnType.PARTITION_KEY),
    GW_ADAPTER_ID_KEY("adapter_id", Type.INT16, KuduColumnType.CLUSTER_COLUMN),
    GW_SORT_KEY("sort", Type.BINARY, KuduColumnType.CLUSTER_COLUMN),
    GW_DATA_ID_KEY("data_id", Type.BINARY, KuduColumnType.CLUSTER_COLUMN),
    GW_FIELD_VISIBILITY_KEY("vis", Type.BINARY, KuduColumnType.CLUSTER_COLUMN),
    GW_NANO_TIME_KEY("nano_time", Type.BINARY, KuduColumnType.CLUSTER_COLUMN),
    GW_FIELD_MASK_KEY("field_mask", Type.BINARY, KuduColumnType.OTHER_COLUMN),
    GW_VALUE_KEY("value", Type.BINARY, KuduColumnType.OTHER_COLUMN),
    GW_NUM_DUPLICATES_KEY("num_duplicates", Type.INT8, KuduColumnType.OTHER_COLUMN);

    private final String fieldName;
    private final Type dataType;
    private KuduColumnType columnType;

    KuduField(final String fieldName, final Type dataType, final KuduColumnType columnType) {
      this.fieldName = fieldName;
      this.dataType = dataType;
      this.columnType = columnType;
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
    partitionKey = row.getBinaryCopy(KuduField.GW_PARTITION_ID_KEY.getFieldName());
    adapterId = row.getShort(KuduField.GW_ADAPTER_ID_KEY.getFieldName());
    sortKey = row.getBinaryCopy(KuduField.GW_SORT_KEY.getFieldName());
    dataId = row.getBinaryCopy(KuduField.GW_DATA_ID_KEY.getFieldName());
    fieldVisibility = row.getBinaryCopy(KuduField.GW_FIELD_VISIBILITY_KEY.getFieldName());
    nanoTime = row.getBinaryCopy(KuduField.GW_NANO_TIME_KEY.getFieldName());
    fieldMask = row.getBinaryCopy(KuduField.GW_FIELD_MASK_KEY.getFieldName());
    value = row.getBinaryCopy(KuduField.GW_VALUE_KEY.getFieldName());
    numDuplicates = row.getByte(KuduField.GW_NUM_DUPLICATES_KEY.getFieldName());
  }

  public KuduRow(final GeoWaveRow row, final GeoWaveValue value) {
    final ByteBuffer nanoBuffer = ByteBuffer.allocate(8);
    nanoBuffer.putLong(0, Long.MAX_VALUE - System.nanoTime());
    partitionKey = row.getPartitionKey();
    adapterId = row.getAdapterId();
    sortKey = row.getSortKey();
    dataId = row.getDataId();
    numDuplicates = row.getNumberOfDuplicates();
    nanoTime = nanoBuffer.array();
    fieldVisibility = value.getVisibility();
    fieldMask = value.getFieldMask();
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
  public void populatePartialRow(final PartialRow partialRow) {
    populatePartialRowPrimaryKey(partialRow);
    partialRow.addBinary(KuduField.GW_FIELD_MASK_KEY.getFieldName(), fieldMask);
    partialRow.addBinary(KuduField.GW_VALUE_KEY.getFieldName(), value);
    partialRow.addByte(KuduField.GW_NUM_DUPLICATES_KEY.getFieldName(), (byte) numDuplicates);
  }

  @Override
  public void populatePartialRowPrimaryKey(final PartialRow partialRow) {
    partialRow.addBinary(KuduField.GW_PARTITION_ID_KEY.getFieldName(), partitionKey);
    partialRow.addShort(KuduField.GW_ADAPTER_ID_KEY.getFieldName(), adapterId);
    partialRow.addBinary(KuduField.GW_SORT_KEY.getFieldName(), sortKey);
    partialRow.addBinary(KuduField.GW_DATA_ID_KEY.getFieldName(), dataId);
    partialRow.addBinary(KuduField.GW_FIELD_VISIBILITY_KEY.getFieldName(), fieldVisibility);
    partialRow.addBinary(KuduField.GW_NANO_TIME_KEY.getFieldName(), nanoTime);
  }
}
