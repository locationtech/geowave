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
import org.apache.kudu.Type;
import org.apache.log4j.Logger;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import java.nio.ByteBuffer;
import java.util.List;

public class KuduMetadataRow {
  private static final Logger LOGGER = Logger.getLogger(KuduMetadataRow.class);

  private final byte[] primaryId;
  private final byte[] secondaryId;
  private final byte[] timestamp;
  private final byte[] visibility;
  private final byte[] value;

  public enum KuduMetadataField {
    GW_PRIMARY_ID_KEY("primary_id", Type.BINARY, KuduColumnType.PARTITION_KEY),
    GW_SECONDARY_ID_KEY("secondary_id", Type.BINARY, KuduColumnType.CLUSTER_COLUMN),
    GW_TIMESTAMP_KEY("timestamp", Type.BINARY, KuduColumnType.CLUSTER_COLUMN),
    GW_VISIBILITY_KEY("visibility", Type.BINARY, KuduColumnType.OTHER_COLUMN),
    GW_VALUE_KEY("value", Type.BINARY, KuduColumnType.OTHER_COLUMN);

    private final String fieldName;
    private final Type dataType;
    private final KuduColumnType columnType;

    KuduMetadataField(
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

  public KuduMetadataRow(final GeoWaveMetadata metadata) {
    this.primaryId = metadata.getPrimaryId();
    this.secondaryId = metadata.getSecondaryId();
    this.visibility = metadata.getVisibility();
    this.value = metadata.getValue();
    ByteBuffer timestampBuffer = ByteBuffer.allocate(8);
    timestampBuffer.putLong(System.nanoTime());
    this.timestamp = timestampBuffer.array();
  }

  public byte[] getPrimaryId() {
    return primaryId;
  }

  public byte[] getSecondaryId() {
    return secondaryId;
  }

  public byte[] getVisibility() {
    return visibility;
  }

  public byte[] getValue() {
    return value;
  }

  public byte[] getTimestamp() {
    return timestamp;
  }

}
