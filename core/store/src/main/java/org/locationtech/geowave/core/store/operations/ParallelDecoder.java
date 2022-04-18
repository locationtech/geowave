/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;

/**
 * An abstract class that offers data stores a way to scan and decode rows in parallel. It is up to
 * the data store implementation to provide implementations of {@link ParallelDecoder.RowProvider}
 * to be used for providing rows from the underlying database.
 *
 * <p> Note: The row transformer passed in MUST be thread-safe, as decoding happens in parallel.
 *
 * @param <T> the type of the decoded rows
 */
public abstract class ParallelDecoder<T> implements CloseableIterator<T> {
  private BlockingQueue<Object> results;
  private ExecutorService threadPool;
  private final GeoWaveRowIteratorTransformer<T> rowTransformer;
  private static int RESULT_BUFFER_SIZE = 10000;
  private int remainingTasks = 0;
  private final int numThreads;
  private static Object TASK_END_MARKER = new Object();

  private Exception exception = null;

  /**
   * Create a parallel decoder with the given row transformer.
   *
   * @param rowTransformer the thread-safe row transformer to use for decoding rows
   */
  public ParallelDecoder(final GeoWaveRowIteratorTransformer<T> rowTransformer) {
    this(rowTransformer, 8);
  }

  /**
   * Create a parallel decoder with the given row transformer and number of threads.
   *
   * @param rowTransformer the thread-safe row transformer to use for decoding rows
   * @param numThreads the number of threads to allow in the thread pool
   */
  public ParallelDecoder(
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final int numThreads) {
    this.numThreads = numThreads;
    this.rowTransformer = rowTransformer;
    this.threadPool =
        new ThreadPoolExecutor(
            numThreads,
            numThreads,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            Executors.defaultThreadFactory());
    ((ThreadPoolExecutor) this.threadPool).allowCoreThreadTimeOut(true);
    results = new ArrayBlockingQueue<>(RESULT_BUFFER_SIZE);
  }

  /** @return the number of threads allowed in the thread pool */
  protected int getNumThreads() {
    return numThreads;
  }

  /**
   * @return a list of {@link RowProvider}s that provide {@link GeoWaveRow}s to the decoder
   * @throws Exception
   */
  protected abstract List<RowProvider> getRowProviders() throws Exception;

  protected synchronized void setDecodeException(final Exception e) {
    if (exception == null) {
      this.exception = e;
      this.threadPool.shutdownNow();
    }
  }

  private synchronized boolean hasException() {
    return this.exception != null;
  }

  private synchronized Exception getException() {
    return this.exception;
  }

  /**
   * Start the parallel decode.
   *
   * @throws Exception
   */
  public void startDecode() throws Exception {
    final List<RowProvider> rowProviders = getRowProviders();
    remainingTasks = rowProviders.size();
    for (final RowProvider rowProvider : rowProviders) {
      threadPool.submit(new DecodeTask<>(rowProvider, this));
    }
  }

  /**
   * Task to decode the rows from a single row provider.
   *
   * @param <T> the type of the decoded rows
   */
  private static class DecodeTask<T> implements Runnable {

    private final RowProvider rowProvider;
    private final ParallelDecoder<T> parent;

    public DecodeTask(final RowProvider rowProvider, final ParallelDecoder<T> parent) {
      this.rowProvider = rowProvider;
      this.parent = parent;
    }

    private boolean shouldTerminate() {
      return Thread.currentThread().isInterrupted();
    }

    private void offerResult(final Object result) throws InterruptedException {
      while (!shouldTerminate() && !parent.results.offer(result)) {
        // Results buffer is full, wait until there is some space
        Thread.sleep(1);
      }
    }

    @Override
    public void run() {
      try {
        rowProvider.init();
        final Iterator<T> transformed = parent.rowTransformer.apply(rowProvider);
        while (transformed.hasNext() && !shouldTerminate()) {
          offerResult(transformed.next());
        }
        // No more rows, signal the end of this task.
        offerResult(TASK_END_MARKER);
      } catch (final Exception e) {
        // Don't overwrite the original exception if there is one
        if (!parent.hasException()) {
          parent.setDecodeException(e);
        }
      } finally {
        try {
          rowProvider.close();
        } catch (final IOException e) {
          // Ignore
        }
      }
    }
  }

  @Override
  public void close() {
    threadPool.shutdownNow();
  }

  Object nextResult = null;

  private void computeNext() {
    try {
      nextResult = null;
      while (remainingTasks > 0) {
        while (!hasException() && ((nextResult = results.poll()) == null)) {
          // No results available, but there are still tasks running,
          // wait for more results.
          Thread.sleep(1);
        }
        // task end was signaled, reduce remaining task count.
        if (nextResult == TASK_END_MARKER) {
          remainingTasks--;
          nextResult = null;
          continue;
        }
        break;
      }
    } catch (final InterruptedException e) {
      setDecodeException(e);
    }
    if (hasException()) {
      throw new RuntimeException(getException());
    }
  }

  @Override
  public boolean hasNext() {
    if (nextResult == null) {
      computeNext();
    }
    return nextResult != null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T next() {
    if (nextResult == null) {
      computeNext();
    }
    final Object next = nextResult;
    nextResult = null;
    return (T) next;
  }

  /**
   * Row provider used by the parallel decoder to get {@link GeoWaveRow}s from the underlying
   * database.
   */
  public abstract static class RowProvider implements Closeable, Iterator<GeoWaveRow> {
    public abstract void init();
  }
}
