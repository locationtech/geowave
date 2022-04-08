/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.metadata.MetadataIterators;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.kudu.KuduMetadataRow.KuduMetadataField;
import org.locationtech.geowave.datastore.kudu.util.KuduUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;

public class KuduMetadataReader implements MetadataReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(KuduMetadataReader.class);
  private final KuduOperations operations;
  private final MetadataType metadataType;

  public KuduMetadataReader(final KuduOperations operations, final MetadataType metadataType) {
    this.operations = operations;
    this.metadataType = metadataType;
  }

  @Override
  public CloseableIterator<GeoWaveMetadata> query(final MetadataQuery query) {
    final List<RowResultIterator> queryResult = new ArrayList<>();
    final String tableName = operations.getMetadataTableName(metadataType);
    try {
      final KuduTable table = operations.getTable(tableName);
      final Schema schema = table.getSchema();
      if (query.hasPrimaryIdRanges()) {
        Arrays.stream(query.getPrimaryIdRanges()).forEach(r -> {
          KuduScanner.KuduScannerBuilder scannerBuilder = operations.getScannerBuilder(table);
          if (r.getStart() != null) {
            final KuduPredicate primaryLowerPred =
                KuduPredicate.newComparisonPredicate(
                    schema.getColumn(KuduMetadataField.GW_PRIMARY_ID_KEY.getFieldName()),
                    KuduPredicate.ComparisonOp.GREATER_EQUAL,
                    r.getStart());
            scannerBuilder = scannerBuilder.addPredicate(primaryLowerPred);
          }
          if (r.getEnd() != null) {
            final KuduPredicate primaryUpperPred =
                KuduPredicate.newComparisonPredicate(
                    schema.getColumn(KuduMetadataField.GW_PRIMARY_ID_KEY.getFieldName()),
                    KuduPredicate.ComparisonOp.LESS,
                    r.getEndAsNextPrefix());
            scannerBuilder = scannerBuilder.addPredicate(primaryUpperPred);
          }

          if (query.hasSecondaryId()) {
            final KuduPredicate secondaryPred =
                KuduPredicate.newComparisonPredicate(
                    schema.getColumn(KuduMetadataField.GW_SECONDARY_ID_KEY.getFieldName()),
                    KuduPredicate.ComparisonOp.EQUAL,
                    query.getSecondaryId());
            scannerBuilder = scannerBuilder.addPredicate(secondaryPred);
          }
          final KuduScanner scanner = scannerBuilder.build();
          KuduUtils.executeQuery(scanner, queryResult);
        });
      } else {
        KuduScanner.KuduScannerBuilder scannerBuilder = operations.getScannerBuilder(table);
        if (query.hasPrimaryId()) {
          if (metadataType.equals(MetadataType.STATISTICS)
              || metadataType.equals(MetadataType.STATISTIC_VALUES)) {
            final KuduPredicate primaryLowerPred =
                KuduPredicate.newComparisonPredicate(
                    schema.getColumn(KuduMetadataField.GW_PRIMARY_ID_KEY.getFieldName()),
                    KuduPredicate.ComparisonOp.GREATER_EQUAL,
                    query.getPrimaryId());
            final KuduPredicate primaryUpperPred =
                KuduPredicate.newComparisonPredicate(
                    schema.getColumn(KuduMetadataField.GW_PRIMARY_ID_KEY.getFieldName()),
                    KuduPredicate.ComparisonOp.LESS,
                    ByteArrayUtils.getNextPrefix(query.getPrimaryId()));
            scannerBuilder =
                scannerBuilder.addPredicate(primaryLowerPred).addPredicate(primaryUpperPred);
          } else {
            final KuduPredicate primaryEqualsPred =
                KuduPredicate.newComparisonPredicate(
                    schema.getColumn(KuduMetadataField.GW_PRIMARY_ID_KEY.getFieldName()),
                    KuduPredicate.ComparisonOp.EQUAL,
                    query.getPrimaryId());
            scannerBuilder = scannerBuilder.addPredicate(primaryEqualsPred);
          }
        }
        if (query.hasSecondaryId()) {
          final KuduPredicate secondaryPred =
              KuduPredicate.newComparisonPredicate(
                  schema.getColumn(KuduMetadataField.GW_SECONDARY_ID_KEY.getFieldName()),
                  KuduPredicate.ComparisonOp.EQUAL,
                  query.getSecondaryId());
          scannerBuilder = scannerBuilder.addPredicate(secondaryPred);
        }
        final KuduScanner scanner = scannerBuilder.build();
        KuduUtils.executeQuery(scanner, queryResult);
      }
    } catch (final KuduException e) {
      LOGGER.error("Encountered error while reading metadata row", e);
    }
    final Iterator<GeoWaveMetadata> temp =
        Streams.stream(Iterators.concat(queryResult.iterator())).map(
            result -> new GeoWaveMetadata(
                (query.hasPrimaryId() && query.isExact()) ? query.getPrimaryId()
                    : result.getBinaryCopy(KuduMetadataField.GW_PRIMARY_ID_KEY.getFieldName()),
                query.hasSecondaryId() ? query.getSecondaryId()
                    : result.getBinaryCopy(KuduMetadataField.GW_SECONDARY_ID_KEY.getFieldName()),
                getVisibility(result),
                result.getBinaryCopy(KuduMetadataField.GW_VALUE_KEY.getFieldName()))).iterator();
    final CloseableIterator<GeoWaveMetadata> retVal = new CloseableIterator.Wrapper<>(temp);
    if (metadataType.isStatValues()) {
      return MetadataIterators.clientVisibilityFilter(retVal, query.getAuthorizations());
    }
    return retVal;
  }

  private byte[] getVisibility(final RowResult result) {
    if (metadataType.isStatValues()) {
      return result.getBinaryCopy(KuduMetadataField.GW_VISIBILITY_KEY.getFieldName());
    }
    return null;
  }

}
