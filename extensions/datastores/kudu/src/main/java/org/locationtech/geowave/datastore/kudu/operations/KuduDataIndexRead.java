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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.datastore.kudu.KuduDataIndexRow;
import org.locationtech.geowave.datastore.kudu.KuduDataIndexRow.KuduDataIndexField;
import org.locationtech.geowave.datastore.kudu.util.KuduUtils;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;

public class KuduDataIndexRead<T> {
  private final Schema schema;
  private final short adapterId;
  final byte[][] dataIds;
  private final KuduTable table;
  private final KuduOperations operations;
  private final boolean visibilityEnabled;
  private final Predicate<GeoWaveRow> filter;
  private List<RowResultIterator> results;

  protected KuduDataIndexRead(
      final short adapterId,
      final byte[][] dataIds,
      final KuduTable table,
      final KuduOperations operations,
      final boolean visibilityEnabled,
      final Predicate<GeoWaveRow> filter) {
    this.adapterId = adapterId;
    this.dataIds = dataIds;
    this.table = table;
    this.schema = table.getSchema();
    this.operations = operations;
    this.visibilityEnabled = visibilityEnabled;
    this.filter = filter;
    this.results = new ArrayList<>();
  }

  public CloseableIterator<T> results() {
    results = new ArrayList<>();
    final KuduPredicate adapterIdPred =
        KuduPredicate.newComparisonPredicate(
            schema.getColumn(KuduDataIndexField.GW_ADAPTER_ID_KEY.getFieldName()),
            ComparisonOp.EQUAL,
            adapterId);
    KuduScannerBuilder scannerBuilder =
        operations.getScannerBuilder(table).addPredicate(adapterIdPred);
    if (dataIds != null) {
      final KuduPredicate partitionPred =
          KuduPredicate.newInListPredicate(
              schema.getColumn(KuduDataIndexField.GW_PARTITION_ID_KEY.getFieldName()),
              Lists.newArrayList(dataIds));
      scannerBuilder = scannerBuilder.addPredicate(partitionPred);
    }
    final KuduScanner scanner = scannerBuilder.build();

    KuduUtils.executeQuery(scanner, results);

    Stream<GeoWaveRow> tmpStream;
    final Iterator<RowResult> concatIterator = Iterators.concat(results.iterator());
    if (dataIds == null) {
      tmpStream =
          Streams.stream(concatIterator).map(
              r -> KuduDataIndexRow.deserializeDataIndexRow(r, visibilityEnabled));
    } else {
      // Order the rows for data index query
      final Map<ByteArray, GeoWaveRow> resultsMap = new HashMap<>();
      while (concatIterator.hasNext()) {
        final RowResult r = concatIterator.next();
        final byte[] d = r.getBinaryCopy(KuduDataIndexField.GW_PARTITION_ID_KEY.getFieldName());
        resultsMap.put(
            new ByteArray(d),
            KuduDataIndexRow.deserializeDataIndexRow(r, visibilityEnabled));
      }
      tmpStream =
          Arrays.stream(dataIds).map(d -> resultsMap.get(new ByteArray(d))).filter(
              Objects::nonNull);
    }

    if (visibilityEnabled) {
      tmpStream = tmpStream.filter(filter);
    }

    return new CloseableIteratorWrapper<>(() -> {
    }, (Iterator<T>) tmpStream.iterator());
  }
}
