/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;

/**
 * An implementation of {@link ParallelDecoder} that consumes a single {@link GeoWaveRow} iterator
 * and decodes it in parallel.
 *
 * @param <T> the type of the decoded rows
 */
public class SimpleParallelDecoder<T> extends ParallelDecoder<T> {
  private ArrayBlockingQueue<GeoWaveRow> consumedRows;
  private Thread consumerThread;
  private volatile boolean isTerminating = false;
  private static final int CONSUMED_ROW_BUFFER_SIZE = 10000;

  public SimpleParallelDecoder(
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final Iterator<GeoWaveRow> sourceIterator) {
    super(rowTransformer);
    consumedRows = new ArrayBlockingQueue<>(CONSUMED_ROW_BUFFER_SIZE);
    consumerThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          while (sourceIterator.hasNext() && !Thread.interrupted()) {
            final GeoWaveRow next = sourceIterator.next();
            while (!consumedRows.offer(next)) {
              // queue is full, wait for space
              try {
                Thread.sleep(1);
              } catch (final InterruptedException e) {
                isTerminating = true;
                return;
              }
            }
          }
        } catch (final Exception e) {
          setDecodeException(e);
        }
        isTerminating = true;
      }
    });
    consumerThread.setDaemon(true);
  }

  @Override
  public void startDecode() throws Exception {
    consumerThread.start();
    super.startDecode();
  }

  @Override
  public void close() {
    if (consumerThread.isAlive()) {
      consumerThread.interrupt();
    }
    super.close();
  }

  @Override
  protected List<RowProvider> getRowProviders() throws Exception {
    final int numThreads = getNumThreads();
    final List<RowProvider> rowProviders = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      rowProviders.add(new BlockingQueueRowProvider<>(this));
    }
    return rowProviders;
  }

  /*
   * Simple row provider that provides the next result from the blocking queue.
   */
  private static class BlockingQueueRowProvider<T> extends ParallelDecoder.RowProvider {

    private final SimpleParallelDecoder<T> source;

    public BlockingQueueRowProvider(final SimpleParallelDecoder<T> source) {
      this.source = source;
    }

    @Override
    public void close() throws IOException {
      // Do nothing
    }

    private GeoWaveRow next = null;

    private void computeNext() {
      while ((next = source.consumedRows.poll()) == null) {
        if (source.isTerminating) {
          next = source.consumedRows.poll();
          break;
        }
        try {
          Thread.sleep(1);
        } catch (final InterruptedException e) {
          return;
        }
      }
    }

    @Override
    public boolean hasNext() {
      if (next == null) {
        computeNext();
      }
      return next != null;
    }

    @Override
    public GeoWaveRow next() {
      if (next == null) {
        computeNext();
      }
      final GeoWaveRow retVal = next;
      next = null;
      return retVal;
    }

    @Override
    public void init() {
      // Do nothing
    }
  }
}
