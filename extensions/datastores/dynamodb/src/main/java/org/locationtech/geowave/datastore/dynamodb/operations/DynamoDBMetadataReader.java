/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb.operations;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.metadata.MetadataIterators;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.dynamodb.util.DynamoDBUtils;
import org.locationtech.geowave.datastore.dynamodb.util.DynamoDBUtils.NoopClosableIteratorWrapper;
import com.google.common.collect.Iterators;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

public class DynamoDBMetadataReader implements MetadataReader {
  private final DynamoDBOperations operations;
  private final MetadataType metadataType;

  public DynamoDBMetadataReader(
      final DynamoDBOperations operations,
      final MetadataType metadataType) {
    this.operations = operations;
    this.metadataType = metadataType;
  }

  @Override
  public CloseableIterator<GeoWaveMetadata> query(final MetadataQuery query) {
    final String tableName = operations.getMetadataTableName(metadataType);

    final boolean needsVisibility =
        metadataType.isStatValues()
            && operations.getOptions().getBaseOptions().isVisibilityEnabled();
    final Iterator<Map<String, AttributeValue>> iterator;
    if (!query.hasPrimaryIdRanges()) {
      if (query.hasPrimaryId() && query.isExact()) {
        final QueryRequest request =
            primaryIdQuery(
                tableName,
                query.getPrimaryId(),
                query.hasSecondaryId() ? query.getSecondaryId() : null);
        iterator = operations.getClient().queryPaginator(request).items().iterator();
      } else {
        final Map<String, Condition> scanFilter = new HashMap<>();
        if (query.hasPrimaryId()) {
          scanFilter.put(
              DynamoDBOperations.METADATA_PRIMARY_ID_KEY,
              condition(ComparisonOperator.BEGINS_WITH, query.getPrimaryId()));
        }
        if (query.hasSecondaryId()) {
          scanFilter.put(
              DynamoDBOperations.METADATA_SECONDARY_ID_KEY,
              condition(ComparisonOperator.EQ, query.getSecondaryId()));
        }
        iterator = scan(tableName, scanFilter);
      }
    } else {
      iterator = Iterators.concat(Arrays.stream(query.getPrimaryIdRanges()).map(r -> {
        final Map<String, Condition> scanFilter = new HashMap<>();
        if (query.hasSecondaryId()) {
          scanFilter.put(
              DynamoDBOperations.METADATA_SECONDARY_ID_KEY,
              condition(ComparisonOperator.EQ, query.getSecondaryId()));
        }
        final Condition primaryIdCondition = primaryIdRangeCondition(r);
        if (primaryIdCondition != null) {
          scanFilter.put(DynamoDBOperations.METADATA_PRIMARY_ID_KEY, primaryIdCondition);
        }
        return scan(tableName, scanFilter);
      }).iterator());
    }
    return wrapIterator(iterator, query, needsVisibility);
  }

  /** Every entry with the primary ID, and with the secondary ID too when one is given. */
  static QueryRequest primaryIdQuery(
      final String tableName,
      final byte[] primaryId,
      final byte[] secondaryId) {
    final Map<String, AttributeValue> values = new HashMap<>();
    values.put(":priVal", DynamoDBUtils.binaryValue(primaryId));
    final QueryRequest.Builder request =
        QueryRequest.builder().tableName(tableName).keyConditionExpression(
            DynamoDBOperations.METADATA_PRIMARY_ID_KEY + " = :priVal");
    if (secondaryId != null) {
      values.put(":secVal", DynamoDBUtils.binaryValue(secondaryId));
      request.filterExpression(DynamoDBOperations.METADATA_SECONDARY_ID_KEY + " = :secVal");
    }
    return request.expressionAttributeValues(values).build();
  }

  private static Condition primaryIdRangeCondition(final ByteArrayRange r) {
    if (r.getStart() != null) {
      if (r.getEnd() != null) {
        return condition(
            ComparisonOperator.BETWEEN,
            r.getStart(),
            ByteArrayUtils.getNextInclusive(r.getEnd()));
      }
      return condition(ComparisonOperator.GE, r.getStart());
    } else if (r.getEnd() != null) {
      return condition(ComparisonOperator.LT, r.getEndAsNextPrefix());
    }
    return null;
  }

  private static Condition condition(final ComparisonOperator operator, final byte[]... values) {
    return Condition.builder().comparisonOperator(operator).attributeValueList(
        Arrays.stream(values).map(DynamoDBUtils::binaryValue).toArray(
            AttributeValue[]::new)).build();
  }

  private Iterator<Map<String, AttributeValue>> scan(
      final String tableName,
      final Map<String, Condition> scanFilter) {
    final ScanRequest.Builder request = ScanRequest.builder().tableName(tableName);
    if (!scanFilter.isEmpty()) {
      request.scanFilter(scanFilter);
    }
    return operations.getClient().scanPaginator(request.build()).items().iterator();
  }

  private CloseableIterator<GeoWaveMetadata> wrapIterator(
      final Iterator<Map<String, AttributeValue>> source,
      final MetadataQuery query,
      final boolean needsVisibility) {
    if (needsVisibility) {
      return MetadataIterators.clientVisibilityFilter(
          new CloseableIterator.Wrapper<GeoWaveMetadata>(
              Iterators.transform(
                  source,
                  result -> new GeoWaveMetadata(
                      DynamoDBUtils.getPrimaryId(result),
                      DynamoDBUtils.getSecondaryId(result),
                      DynamoDBUtils.getVisibility(result),
                      DynamoDBUtils.getValue(result)))),
          query.getAuthorizations());
    } else {
      return new CloseableIteratorWrapper<>(
          new NoopClosableIteratorWrapper(),
          Iterators.transform(
              source,
              result -> new GeoWaveMetadata(
                  DynamoDBUtils.getPrimaryId(result),
                  DynamoDBUtils.getSecondaryId(result),
                  null,
                  DynamoDBUtils.getValue(result))));
    }
  }
}
