/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb.operations;

import java.util.HashMap;
import java.util.Map;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.datastore.dynamodb.util.DynamoDBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoDBMetadataWriter implements MetadataWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBMetadataWriter.class);

  final DynamoDBOperations operations;
  private final String tableName;
  private long lastWrite = -1;

  public DynamoDBMetadataWriter(final DynamoDBOperations operations, final String tableName) {
    this.operations = operations;
    this.tableName = tableName;
  }

  @Override
  public void close() throws Exception {}

  @Override
  public void write(final GeoWaveMetadata metadata) {
    final Map<String, AttributeValue> map = new HashMap<>();
    map.put(
        DynamoDBOperations.METADATA_PRIMARY_ID_KEY,
        DynamoDBUtils.binaryValue(metadata.getPrimaryId()));

    if (metadata.getSecondaryId() != null) {
      map.put(
          DynamoDBOperations.METADATA_SECONDARY_ID_KEY,
          DynamoDBUtils.binaryValue(metadata.getSecondaryId()));
      if ((metadata.getVisibility() != null) && (metadata.getVisibility().length > 0)) {
        map.put(
            DynamoDBOperations.METADATA_VISIBILITY_KEY,
            DynamoDBUtils.binaryValue(metadata.getVisibility()));
      }
    }
    map.put(
        DynamoDBOperations.METADATA_TIMESTAMP_KEY,
        AttributeValue.fromN(Long.toString(safeWrite())));
    map.put(DynamoDBOperations.METADATA_VALUE_KEY, DynamoDBUtils.binaryValue(metadata.getValue()));

    operations.getClient().putItem(b -> b.tableName(tableName).item(map));
  }

  private long safeWrite() {
    long time = System.currentTimeMillis();
    while (time <= lastWrite) {
      try {
        Thread.sleep(10);
        time = System.currentTimeMillis();
      } catch (final InterruptedException e) {
        LOGGER.warn("Unable to wait for new time", e);
      }
    }
    lastWrite = time;
    return time;
  }

  @Override
  public void flush() {}
}
