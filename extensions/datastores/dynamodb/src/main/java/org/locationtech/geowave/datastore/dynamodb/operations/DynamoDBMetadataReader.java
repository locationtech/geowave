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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
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
import org.locationtech.geowave.datastore.dynamodb.util.LazyPaginatedScan;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.collect.Iterators;

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
        final QueryRequest queryRequest = new QueryRequest(tableName);

        if (query.hasSecondaryId()) {
          queryRequest.withFilterExpression(
              DynamoDBOperations.METADATA_SECONDARY_ID_KEY
                  + " = :secVal").addExpressionAttributeValuesEntry(
                      ":secVal",
                      new AttributeValue().withB(ByteBuffer.wrap(query.getSecondaryId())));
        }
        queryRequest.withKeyConditionExpression(
            DynamoDBOperations.METADATA_PRIMARY_ID_KEY
                + " = :priVal").addExpressionAttributeValuesEntry(
                    ":priVal",
                    new AttributeValue().withB(ByteBuffer.wrap(query.getPrimaryId())));

        final QueryResult queryResult = operations.getClient().query(queryRequest);

        return wrapIterator(queryResult.getItems().iterator(), query, needsVisibility);
      }

      final ScanRequest scan = new ScanRequest(tableName);
      if (query.hasPrimaryId()) {
        scan.addScanFilterEntry(
            DynamoDBOperations.METADATA_PRIMARY_ID_KEY,
            new Condition().withAttributeValueList(
                new AttributeValue().withB(
                    ByteBuffer.wrap(query.getPrimaryId()))).withComparisonOperator(
                        ComparisonOperator.BEGINS_WITH));
      }
      if (query.hasSecondaryId()) {
        scan.addScanFilterEntry(
            DynamoDBOperations.METADATA_SECONDARY_ID_KEY,
            new Condition().withAttributeValueList(
                new AttributeValue().withB(
                    ByteBuffer.wrap(query.getSecondaryId()))).withComparisonOperator(
                        ComparisonOperator.EQ));
      }
      final ScanResult scanResult = operations.getClient().scan(scan);

      iterator = new LazyPaginatedScan(scanResult, scan, operations.getClient());
    } else {
      iterator = Iterators.concat(Arrays.stream(query.getPrimaryIdRanges()).map(r -> {
        final ScanRequest scan = new ScanRequest(tableName);
        if (query.hasSecondaryId()) {
          scan.addScanFilterEntry(
              DynamoDBOperations.METADATA_SECONDARY_ID_KEY,
              new Condition().withAttributeValueList(
                  new AttributeValue().withB(
                      ByteBuffer.wrap(query.getSecondaryId()))).withComparisonOperator(
                          ComparisonOperator.EQ));
        }
        if (r.getStart() != null) {
          if (r.getEnd() != null) {
            scan.addScanFilterEntry(
                DynamoDBOperations.METADATA_PRIMARY_ID_KEY,
                new Condition().withAttributeValueList(
                    new AttributeValue().withB(ByteBuffer.wrap(r.getStart())),
                    new AttributeValue().withB(
                        ByteBuffer.wrap(
                            ByteArrayUtils.getNextInclusive(r.getEnd())))).withComparisonOperator(
                                ComparisonOperator.BETWEEN));

          } else {
            scan.addScanFilterEntry(
                DynamoDBOperations.METADATA_PRIMARY_ID_KEY,
                new Condition().withAttributeValueList(
                    new AttributeValue().withB(
                        ByteBuffer.wrap(r.getStart()))).withComparisonOperator(
                            ComparisonOperator.GE));
          }
        } else if (r.getEnd() != null) {
          scan.addScanFilterEntry(
              DynamoDBOperations.METADATA_PRIMARY_ID_KEY,
              new Condition().withAttributeValueList(
                  new AttributeValue().withB(
                      ByteBuffer.wrap(r.getEndAsNextPrefix()))).withComparisonOperator(
                          ComparisonOperator.LT));
        }
        final ScanResult scanResult = operations.getClient().scan(scan);
        return new LazyPaginatedScan(scanResult, scan, operations.getClient());
      }).iterator());
    }
    return wrapIterator(iterator, query, needsVisibility);
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
