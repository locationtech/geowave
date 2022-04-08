/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra;

import java.util.Arrays;
import java.util.function.BiFunction;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.entities.MergeableGeoWaveRow;
import org.locationtech.geowave.datastore.cassandra.util.CassandraUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.OngoingPartitionKey;

public class CassandraRow extends MergeableGeoWaveRow {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRow.class);

  private static enum ColumnType {
    PARTITION_KEY((
        final OngoingPartitionKey c,
        final Pair<String, DataType> f) -> c.withPartitionKey(f.getLeft(), f.getRight())),
    CLUSTER_COLUMN((final CreateTable c, final Pair<String, DataType> f) -> c.withClusteringColumn(
        f.getLeft(),
        f.getRight()), null),
    OTHER_COLUMN((final CreateTable c, final Pair<String, DataType> f) -> c.withClusteringColumn(
        f.getLeft(),
        f.getRight()), null);

    private BiFunction<CreateTable, Pair<String, DataType>, CreateTable> createFunction;
    private BiFunction<OngoingPartitionKey, Pair<String, DataType>, CreateTable> createPartitionKeyFunction;

    private ColumnType(
        final BiFunction<OngoingPartitionKey, Pair<String, DataType>, CreateTable> createPartitionKeyFunction) {
      this(
          (final CreateTable c, final Pair<String, DataType> f) -> createPartitionKeyFunction.apply(
              c,
              f),
          createPartitionKeyFunction);
    }

    private ColumnType(
        final BiFunction<CreateTable, Pair<String, DataType>, CreateTable> createFunction,
        final BiFunction<OngoingPartitionKey, Pair<String, DataType>, CreateTable> createPartitionKeyFunction) {
      this.createFunction = createFunction;
      this.createPartitionKeyFunction = createPartitionKeyFunction;
    }
  }

  public static enum CassandraField {
    GW_PARTITION_ID_KEY("partition", DataTypes.BLOB, ColumnType.PARTITION_KEY, true),
    GW_ADAPTER_ID_KEY("adapter_id", DataTypes.SMALLINT, ColumnType.CLUSTER_COLUMN, true),
    GW_SORT_KEY("sort", DataTypes.BLOB, ColumnType.CLUSTER_COLUMN),
    GW_DATA_ID_KEY("data_id", DataTypes.BLOB, ColumnType.CLUSTER_COLUMN),
    GW_FIELD_VISIBILITY_KEY("vis", DataTypes.BLOB, ColumnType.CLUSTER_COLUMN),
    GW_NANO_TIME_KEY("nano_time", DataTypes.BLOB, ColumnType.CLUSTER_COLUMN),
    GW_FIELD_MASK_KEY("field_mask", DataTypes.BLOB, ColumnType.OTHER_COLUMN),
    GW_VALUE_KEY("value", DataTypes.BLOB, ColumnType.OTHER_COLUMN, true),
    GW_NUM_DUPLICATES_KEY("num_duplicates", DataTypes.TINYINT, ColumnType.OTHER_COLUMN);

    private final String fieldName;
    private final DataType dataType;
    private ColumnType columnType;
    private final boolean isDataIndexColumn;

    private CassandraField(
        final String fieldName,
        final DataType dataType,
        final ColumnType columnType) {
      this(fieldName, dataType, columnType, false);
    }

    private CassandraField(
        final String fieldName,
        final DataType dataType,
        final ColumnType columnType,
        final boolean isDataIndexColumn) {
      this.fieldName = fieldName;
      this.dataType = dataType;
      this.columnType = columnType;
      this.isDataIndexColumn = isDataIndexColumn;
    }

    public boolean isDataIndexColumn() {
      return isDataIndexColumn;
    }

    public boolean isPartitionKey() {
      return columnType.equals(ColumnType.PARTITION_KEY);
    }

    public String getFieldName() {
      return fieldName;
    }

    public String getBindMarkerName() {
      return fieldName + "_val";
    }

    public String getLowerBoundBindMarkerName() {
      return fieldName + "_min";
    }

    public String getUpperBoundBindMarkerName() {
      return fieldName + "_max";
    }

    public CreateTable addColumn(final CreateTable create) {
      return columnType.createFunction.apply(create, Pair.of(fieldName, dataType));
    }

    public CreateTable addPartitionKey(final CreateTableStart start) {
      return columnType.createPartitionKeyFunction.apply(start, Pair.of(fieldName, dataType));
    }
  }

  private final Row row;

  public CassandraRow() {
    super(new GeoWaveValue[0]);
    row = null;
  }

  public CassandraRow(final Row row) {
    super(getFieldValues(row));
    this.row = row;
  }

  @Override
  public byte[] getDataId() {
    return row.getByteBuffer(CassandraField.GW_DATA_ID_KEY.getFieldName()).array();
  }

  @Override
  public byte[] getSortKey() {
    return row.getByteBuffer(CassandraField.GW_SORT_KEY.getFieldName()).array();
  }

  @Override
  public byte[] getPartitionKey() {
    final byte[] partitionKey =
        row.getByteBuffer(CassandraField.GW_PARTITION_ID_KEY.getFieldName()).array();
    if (Arrays.equals(CassandraUtils.EMPTY_PARTITION_KEY, partitionKey)) {
      // we shouldn't expose the reserved "empty" partition key externally
      return new byte[0];
    }
    return partitionKey;
  }

  @Override
  public int getNumberOfDuplicates() {
    return row.getByte(CassandraField.GW_NUM_DUPLICATES_KEY.getFieldName());
  }

  private static GeoWaveValue[] getFieldValues(final Row row) {
    final byte[] fieldMask =
        row.getByteBuffer(CassandraField.GW_FIELD_MASK_KEY.getFieldName()).array();
    final byte[] value = row.getByteBuffer(CassandraField.GW_VALUE_KEY.getFieldName()).array();
    final byte[] visibility =
        row.getByteBuffer(CassandraField.GW_FIELD_VISIBILITY_KEY.getFieldName()).array();

    final GeoWaveValue[] fieldValues = new GeoWaveValueImpl[1];
    fieldValues[0] = new GeoWaveValueImpl(fieldMask, visibility, value);
    return fieldValues;
  }

  @Override
  public short getAdapterId() {
    return row.getShort(CassandraField.GW_ADAPTER_ID_KEY.getFieldName());
  }
}
