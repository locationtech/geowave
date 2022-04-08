/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;

public class KuduDataIndexRow implements PersistentKuduRow {

  private final byte[] partitionKey;
  private final short adapterId;
  private final byte[] value;

  public enum KuduDataIndexField {
    GW_PARTITION_ID_KEY("partition", Type.BINARY, KuduColumnType.PARTITION_KEY),
    GW_ADAPTER_ID_KEY("adapter_id", Type.INT16, KuduColumnType.CLUSTER_COLUMN),
    GW_VALUE_KEY("value", Type.BINARY, KuduColumnType.OTHER_COLUMN);

    private final String fieldName;
    private final Type dataType;
    private KuduColumnType columnType;

    KuduDataIndexField(
        final String fieldName,
        final Type dataType,
        final KuduColumnType columnType) {
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

  public KuduDataIndexRow(final byte[] partitionKey, final short adapterId, final byte[] value) {
    this.partitionKey = partitionKey;
    this.adapterId = adapterId;
    this.value = value;
  }

  public KuduDataIndexRow(
      final GeoWaveRow row,
      final GeoWaveValue value,
      final boolean isVisibilityEnabled) {
    this(
        row.getDataId(),
        row.getAdapterId(),
        DataIndexUtils.serializeDataIndexValue(value, isVisibilityEnabled));
  }

  public byte[] getPartitionKey() {
    return partitionKey;
  }

  public short getAdapterId() {
    return adapterId;
  }

  @Override
  public void populatePartialRow(final PartialRow partialRow) {
    populatePartialRowPrimaryKey(partialRow);
    partialRow.addBinary(KuduDataIndexField.GW_VALUE_KEY.getFieldName(), value);
  }

  @Override
  public void populatePartialRowPrimaryKey(final PartialRow partialRow) {
    partialRow.addBinary(KuduDataIndexField.GW_PARTITION_ID_KEY.getFieldName(), partitionKey);
    partialRow.addShort(KuduDataIndexField.GW_ADAPTER_ID_KEY.getFieldName(), adapterId);
  }

  public static GeoWaveRow deserializeDataIndexRow(
      final RowResult row,
      final boolean isVisibilityEnabled) {
    return DataIndexUtils.deserializeDataIndexRow(
        row.getBinaryCopy(KuduDataIndexField.GW_PARTITION_ID_KEY.getFieldName()),
        row.getShort(KuduDataIndexField.GW_ADAPTER_ID_KEY.getFieldName()),
        row.getBinaryCopy(KuduDataIndexField.GW_VALUE_KEY.getFieldName()),
        isVisibilityEnabled);
  }
}
