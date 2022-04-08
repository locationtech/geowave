/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb.operations;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;

public class DynamoDBMetadataDeleter implements MetadataDeleter {

  private final DynamoDBOperations operations;
  private final MetadataType metadataType;

  public DynamoDBMetadataDeleter(
      final DynamoDBOperations operations,
      final MetadataType metadataType) {
    super();
    this.operations = operations;
    this.metadataType = metadataType;
  }

  @Override
  public void close() throws Exception {}

  @Override
  public boolean delete(final MetadataQuery metadata) {
    // the nature of metadata deleter is that primary ID is always
    // well-defined and it is deleting a single entry at a time
    final String tableName = operations.getMetadataTableName(metadataType);
    if (!metadata.hasPrimaryId() && !metadata.hasSecondaryId()) {
      if (operations.getOptions().getBaseOptions().isVisibilityEnabled()) {
        // we need to respect visibilities although this may be much slower
        DataStoreUtils.safeMetadataDelete(this, operations, metadataType, metadata);
      } else {
        // without visibilities it is much faster to drop the table
        operations.dropMetadataTable(metadataType);
      }
      return true;
    }
    final QueryRequest queryRequest = new QueryRequest(tableName);

    if (metadata.hasSecondaryId()) {
      queryRequest.withFilterExpression(
          DynamoDBOperations.METADATA_SECONDARY_ID_KEY
              + " = :secVal").addExpressionAttributeValuesEntry(
                  ":secVal",
                  new AttributeValue().withB(ByteBuffer.wrap(metadata.getSecondaryId())));
    }
    if (metadata.hasPrimaryId()) {
      queryRequest.withKeyConditionExpression(
          DynamoDBOperations.METADATA_PRIMARY_ID_KEY
              + " = :priVal").addExpressionAttributeValuesEntry(
                  ":priVal",
                  new AttributeValue().withB(ByteBuffer.wrap(metadata.getPrimaryId())));
    }

    final QueryResult queryResult = operations.getClient().query(queryRequest);
    for (final Map<String, AttributeValue> entry : queryResult.getItems()) {
      final Map<String, AttributeValue> key = new HashMap<>();
      key.put(
          DynamoDBOperations.METADATA_PRIMARY_ID_KEY,
          entry.get(DynamoDBOperations.METADATA_PRIMARY_ID_KEY));
      key.put(
          DynamoDBOperations.METADATA_TIMESTAMP_KEY,
          entry.get(DynamoDBOperations.METADATA_TIMESTAMP_KEY));
      operations.getClient().deleteItem(tableName, key);
    }

    return true;
  }

  @Override
  public void flush() {}
}
