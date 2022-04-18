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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kudu.Schema;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.AsyncKuduScanner.AsyncKuduScannerBuilder;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResultIterator;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveRowMergingIterator;
import org.locationtech.geowave.core.store.util.RowConsumer;
import org.locationtech.geowave.datastore.kudu.KuduRow;
import org.locationtech.geowave.datastore.kudu.KuduRow.KuduField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Streams;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

public class KuduRangeRead<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(KuduRangeRead.class);
  private static final int MAX_CONCURRENT_READ = 100;
  private static final int MAX_BOUNDED_READS_ENQUEUED = 1000000;
  private final Collection<SinglePartitionQueryRanges> ranges;
  private final Schema schema;
  private final short[] adapterIds;
  private final KuduTable table;
  private final KuduOperations operations;
  private final boolean visibilityEnabled;
  private final Predicate<GeoWaveRow> filter;
  private final GeoWaveRowIteratorTransformer<T> rowTransformer;
  private final boolean rowMerging;

  // only allow so many outstanding async reads or writes, use this semaphore
  // to control it
  private final Semaphore readSemaphore = new Semaphore(MAX_CONCURRENT_READ);

  protected KuduRangeRead(
      final Collection<SinglePartitionQueryRanges> ranges,
      final short[] adapterIds,
      final KuduTable table,
      final KuduOperations operations,
      final boolean visibilityEnabled,
      final Predicate<GeoWaveRow> filter,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final boolean rowMerging) {
    this.ranges = ranges;
    this.adapterIds = adapterIds;
    this.table = table;
    this.schema = table.getSchema();
    this.operations = operations;
    this.visibilityEnabled = visibilityEnabled;
    this.filter = filter;
    this.rowTransformer = rowTransformer;
    this.rowMerging = rowMerging;
  }

  public CloseableIterator<T> results() {
    final List<AsyncKuduScanner> scanners = new ArrayList<>();

    final KuduPredicate adapterIdPred =
        KuduPredicate.newInListPredicate(
            schema.getColumn(KuduField.GW_ADAPTER_ID_KEY.getFieldName()),
            Arrays.asList(ArrayUtils.toObject(adapterIds)));
    if ((ranges != null) && !ranges.isEmpty()) {
      for (final SinglePartitionQueryRanges r : ranges) {
        byte[] partitionKey = r.getPartitionKey();
        if (partitionKey == null) {
          partitionKey = new byte[0];
        }
        final KuduPredicate partitionPred =
            KuduPredicate.newComparisonPredicate(
                schema.getColumn(KuduField.GW_PARTITION_ID_KEY.getFieldName()),
                ComparisonOp.EQUAL,
                partitionKey);
        for (final ByteArrayRange range : r.getSortKeyRanges()) {
          final byte[] start = range.getStart() != null ? range.getStart() : new byte[0];
          final byte[] end =
              range.getEnd() != null ? range.getEndAsNextPrefix()
                  : new byte[] {
                      (byte) 0xFF,
                      (byte) 0xFF,
                      (byte) 0xFF,
                      (byte) 0xFF,
                      (byte) 0xFF,
                      (byte) 0xFF,
                      (byte) 0xFF};
          final KuduPredicate lowerPred =
              KuduPredicate.newComparisonPredicate(
                  schema.getColumn(KuduField.GW_SORT_KEY.getFieldName()),
                  ComparisonOp.GREATER_EQUAL,
                  start);
          final KuduPredicate upperPred =
              KuduPredicate.newComparisonPredicate(
                  schema.getColumn(KuduField.GW_SORT_KEY.getFieldName()),
                  ComparisonOp.LESS,
                  end);

          final AsyncKuduScannerBuilder scannerBuilder = operations.getAsyncScannerBuilder(table);
          final AsyncKuduScanner scanner =
              scannerBuilder.addPredicate(adapterIdPred).addPredicate(partitionPred).addPredicate(
                  lowerPred).addPredicate(upperPred).build();
          scanners.add(scanner);
        }
      }
    } else {
      final AsyncKuduScannerBuilder scannerBuilder = operations.getAsyncScannerBuilder(table);
      final AsyncKuduScanner scanner = scannerBuilder.addPredicate(adapterIdPred).build();
      scanners.add(scanner);
    }

    return executeQueryAsync(scanners);
  }

  public CloseableIterator<T> executeQueryAsync(final List<AsyncKuduScanner> scanners) {
    final BlockingQueue<Object> results = new LinkedBlockingQueue<>(MAX_BOUNDED_READS_ENQUEUED);
    final AtomicBoolean isCanceled = new AtomicBoolean(false);
    new Thread(() -> {
      final AtomicInteger queryCount = new AtomicInteger(1);
      for (final AsyncKuduScanner scanner : scanners) {
        try {
          readSemaphore.acquire();
          executeScanner(
              scanner,
              readSemaphore,
              results,
              queryCount,
              isCanceled,
              visibilityEnabled,
              filter,
              rowTransformer,
              rowMerging);
        } catch (final InterruptedException e) {
          LOGGER.warn("Exception while executing query", e);
          readSemaphore.release();
        }
      }
      // then decrement
      if (queryCount.decrementAndGet() <= 0) {
        // and if there are no queries, there may not have been any statements submitted
        try {
          results.put(RowConsumer.POISON);
        } catch (final InterruptedException e) {
          LOGGER.error("Interrupted while finishing blocking queue, this may result in deadlock!");
        }
      }
    }, "Kudu Query Executor").start();
    return new CloseableIteratorWrapper<T>(() -> isCanceled.set(true), new RowConsumer(results));
  }

  public void checkFinalize(
      final AsyncKuduScanner scanner,
      final Semaphore semaphore,
      final BlockingQueue<Object> resultQueue,
      final AtomicInteger queryCount) {
    scanner.close();
    semaphore.release();
    if (queryCount.decrementAndGet() <= 0) {
      try {
        resultQueue.put(RowConsumer.POISON);
      } catch (final InterruptedException e) {
        LOGGER.error("Interrupted while finishing blocking queue, this may result in deadlock!");
      }
    }
  }

  public Deferred<Object> executeScanner(
      final AsyncKuduScanner scanner,
      final Semaphore semaphore,
      final BlockingQueue<Object> resultQueue,
      final AtomicInteger queryCount,
      final AtomicBoolean isCanceled,
      final boolean visibilityEnabled,
      final Predicate<GeoWaveRow> filter,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final boolean rowMerging) {
    // Errback class
    class QueryErrback implements Callback<Deferred<Object>, Exception> {
      @Override
      public Deferred<Object> call(final Exception e) {
        LOGGER.warn("While scanning rows from kudu", e);
        checkFinalize(scanner, semaphore, resultQueue, queryCount);
        return Deferred.fromError(e);
      }
    }

    final QueryErrback errBack = new QueryErrback();

    // callback class
    class QueryCallback implements Callback<Deferred<Object>, RowResultIterator> {
      @Override
      public Deferred<Object> call(final RowResultIterator rs) {
        if ((rs == null) || isCanceled.get()) {
          checkFinalize(scanner, semaphore, resultQueue, queryCount);
          return Deferred.fromResult(null);
        }

        if (rs.getNumRows() > 0) {
          Stream<GeoWaveRow> tmpStream = Streams.stream(rs.iterator()).map(KuduRow::new);
          if (visibilityEnabled) {
            tmpStream = tmpStream.filter(filter);
          }

          final Iterator<GeoWaveRow> tmpIterator = tmpStream.iterator();

          rowTransformer.apply(
              rowMerging ? new GeoWaveRowMergingIterator(tmpIterator)
                  : tmpIterator).forEachRemaining(row -> {
                    try {
                      resultQueue.put(row);
                    } catch (final InterruptedException e) {
                      LOGGER.warn("interrupted while waiting to enqueue a kudu result", e);
                    }
                  });
        }

        if (scanner.hasMoreRows()) {
          return scanner.nextRows().addCallbackDeferring(this).addErrback(errBack);
        }

        checkFinalize(scanner, semaphore, resultQueue, queryCount);
        return Deferred.fromResult(null);
      }
    }

    queryCount.incrementAndGet();
    return scanner.nextRows().addCallbackDeferring(new QueryCallback()).addErrback(errBack);
  }
}
