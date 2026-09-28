/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb.operations;

import java.util.Map;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.datastore.dynamodb.DynamoDBRow;
import com.google.common.collect.Maps;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoDBDeleter implements RowDeleter {
  private final DynamoDBOperations operations;
  private final String tableName;

  public DynamoDBDeleter(final DynamoDBOperations operations, final String qualifiedTableName) {
    this.operations = operations;
    tableName = qualifiedTableName;
  }

  @Override
  public void close() {}

  @Override
  public void delete(final GeoWaveRow row) {
    final DynamoDBRow dynRow = (DynamoDBRow) row;

    for (final Map<String, AttributeValue> attributeMappings : dynRow.getAttributeMapping()) {
      final Map<String, AttributeValue> key =
          Maps.filterKeys(
              attributeMappings,
              name -> DynamoDBRow.GW_PARTITION_ID_KEY.equals(name)
                  || DynamoDBRow.GW_RANGE_KEY.equals(name));
      operations.getClient().deleteItem(b -> b.tableName(tableName).key(key));
    }
  }

  @Override
  public void flush() {
    // Do nothing, delete is done immediately.
  }
}
