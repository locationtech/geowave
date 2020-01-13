/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.operations;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveRowMergingIterator;
import org.locationtech.geowave.core.store.util.RowConsumer;
import org.locationtech.geowave.datastore.cassandra.CassandraRow;
import org.locationtech.geowave.datastore.cassandra.CassandraRow.CassandraField;
import org.locationtech.geowave.datastore.cassandra.util.CassandraUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

public class BatchedRangeRead<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BatchedRangeRead.class);
  private static final int MAX_CONCURRENT_READ = 100;
  private static final int MAX_BOUNDED_READS_ENQUEUED = 1000000;
  private final CassandraOperations operations;
  private final PreparedStatement preparedRead;
  private final Collection<SinglePartitionQueryRanges> ranges;
  private final short[] adapterIds;
  private final GeoWaveRowIteratorTransformer<T> rowTransformer;
  private final boolean rowMerging;
  Predicate<GeoWaveRow> filter;

  // only allow so many outstanding async reads or writes, use this semaphore
  // to control it
  private final Semaphore readSemaphore = new Semaphore(MAX_CONCURRENT_READ);

  protected BatchedRangeRead(
      final PreparedStatement preparedRead,
      final CassandraOperations operations,
      final short[] adapterIds,
      final Collection<SinglePartitionQueryRanges> ranges,
      final boolean rowMerging,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final Predicate<GeoWaveRow> filter) {
    this.preparedRead = preparedRead;
    this.operations = operations;
    this.adapterIds = adapterIds;
    this.ranges = ranges;
    this.rowMerging = rowMerging;
    this.rowTransformer = rowTransformer;
    this.filter = filter;
  }

  public CloseableIterator<T> results() {
    final List<BoundStatement> statements = new ArrayList<>();
    for (final SinglePartitionQueryRanges r : ranges) {
      final byte[] partitionKey = CassandraUtils.getCassandraSafePartitionKey(r.getPartitionKey());
      for (final ByteArrayRange range : r.getSortKeyRanges()) {
        final BoundStatement boundRead = new BoundStatement(preparedRead);
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
        boundRead.set(
            CassandraField.GW_SORT_KEY.getLowerBoundBindMarkerName(),
            ByteBuffer.wrap(start),
            ByteBuffer.class);

        boundRead.set(
            CassandraField.GW_SORT_KEY.getUpperBoundBindMarkerName(),
            ByteBuffer.wrap(end),
            ByteBuffer.class);
        boundRead.set(
            CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName(),
            ByteBuffer.wrap(partitionKey),
            ByteBuffer.class);

        boundRead.set(
            CassandraField.GW_ADAPTER_ID_KEY.getBindMarkerName(),
            Arrays.asList(ArrayUtils.toObject(adapterIds)),
            TypeCodec.list(TypeCodec.smallInt()));
        statements.add(boundRead);
      }
    }
    return executeQueryAsync(statements.toArray(new BoundStatement[] {}));
  }

  public CloseableIterator<T> executeQueryAsync(final Statement... statements) {
    // first create a list of asynchronous query executions
    final List<ResultSetFuture> futures = Lists.newArrayListWithExpectedSize(statements.length);
    final BlockingQueue<Object> results = new LinkedBlockingQueue<>(MAX_BOUNDED_READS_ENQUEUED);
    new Thread(new Runnable() {
      @Override
      public void run() {
        // set it to 1 to make sure all queries are submitted in
        // the loop
        final AtomicInteger queryCount = new AtomicInteger(1);
        for (final Statement s : statements) {
          try {
            readSemaphore.acquire();

            final ResultSetFuture f = operations.getSession().executeAsync(s);
            synchronized (futures) {
              futures.add(f);
            }
            Futures.addCallback(
                f,
                new QueryCallback(
                    queryCount,
                    results,
                    rowTransformer,
                    filter,
                    rowMerging,
                    readSemaphore),
                CassandraOperations.READ_RESPONSE_THREADS);
          } catch (final InterruptedException e) {
            LOGGER.warn("Exception while executing query", e);
            readSemaphore.release();
          }
        }
        // then decrement
        if (queryCount.decrementAndGet() <= 0) {
          // and if there are no queries, there may not have
          // been any
          // statements submitted
          try {
            results.put(RowConsumer.POISON);
          } catch (final InterruptedException e) {
            LOGGER.error(
                "Interrupted while finishing blocking queue, this may result in deadlock!");
          }
        }
      }
    }, "Cassandra Query Executor").start();
    return new CloseableIteratorWrapper<T>(new Closeable() {
      @Override
      public void close() throws IOException {
        synchronized (futures) {
          for (final ResultSetFuture f : futures) {
            f.cancel(true);
          }
        }
      }
    }, new RowConsumer(results));
  }

  // callback class
  protected static class QueryCallback<T> implements FutureCallback<ResultSet> {
    private final Semaphore semaphore;
    private final BlockingQueue<Object> resultQueue;
    private final AtomicInteger queryCount;
    private final boolean rowMerging;

    private final GeoWaveRowIteratorTransformer<T> rowTransform;
    Predicate<GeoWaveRow> filter;

    public QueryCallback(
        final AtomicInteger queryCount,
        final BlockingQueue<Object> resultQueue,
        final GeoWaveRowIteratorTransformer<T> rowTransform,
        final Predicate<GeoWaveRow> filter,
        final boolean rowMerging,
        final Semaphore semaphore) {
      this.queryCount = queryCount;
      this.queryCount.incrementAndGet();
      this.resultQueue = resultQueue;
      this.rowTransform = rowTransform;
      this.filter = filter;
      this.rowMerging = rowMerging;
      this.semaphore = semaphore;
    }

    @Override
    public void onSuccess(final ResultSet result) {
      try {
        if (result != null) {
          final Iterator<GeoWaveRow> iterator =
              (Iterator) Streams.stream(result.iterator()).map(row -> new CassandraRow(row)).filter(
                  filter).iterator();
          rowTransform.apply(
              rowMerging ? new GeoWaveRowMergingIterator(iterator) : iterator).forEachRemaining(
                  row -> {
                    try {
                      resultQueue.put(row);
                    } catch (final InterruptedException e) {
                      LOGGER.warn("interrupted while waiting to enqueue a cassandra result", e);
                    }
                  });
        }
      } finally {
        checkFinalize();
      }
    }

    @Override
    public void onFailure(final Throwable t) {
      checkFinalize();

      // go ahead and wrap in a runtime exception for this case, but you
      // can do logging or start counting errors.
      if (!(t instanceof CancellationException)) {
        LOGGER.error("Failure from async query", t);
        throw new RuntimeException(t);
      }
    }

    private void checkFinalize() {
      semaphore.release();
      if (queryCount.decrementAndGet() <= 0) {
        try {
          resultQueue.put(RowConsumer.POISON);
        } catch (final InterruptedException e) {
          LOGGER.error("Interrupted while finishing blocking queue, this may result in deadlock!");
        }
      }
    }
  }
}
