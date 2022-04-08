/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base.dataidx;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchDataIndexRetrievalIteratorHelper<V, O> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(BatchDataIndexRetrievalIteratorHelper.class);
  private static final NoOp NO_OP = new NoOp();
  private static final int MAX_COMPLETED_OBJECT_CAPACITY = 1000000;
  private final BlockingQueue<Object> completedObjects =
      new LinkedBlockingDeque<>(MAX_COMPLETED_OBJECT_CAPACITY);
  private final AtomicInteger outstandingFutures = new AtomicInteger(0);
  private static final Object POISON = new Object();
  private final AtomicBoolean scannedResultsExhausted = new AtomicBoolean(false);

  private final AtomicBoolean scannedResultsStarted = new AtomicBoolean(false);
  private final BatchDataIndexRetrieval dataIndexRetrieval;

  public BatchDataIndexRetrievalIteratorHelper(final BatchDataIndexRetrieval dataIndexRetrieval) {
    this.dataIndexRetrieval = dataIndexRetrieval;
  }

  public void preHasNext() {
    if (!scannedResultsStarted.getAndSet(true)) {
      dataIndexRetrieval.notifyIteratorInitiated();
    }
  }

  public V postDecodeRow(final V decodedRow) {
    return postDecodeRow(decodedRow, (Function<V, O>) NO_OP);
  }

  public V postDecodeRow(final V decodedRow, final Function<V, O> f) {
    if (decodedRow instanceof CompletableFuture) {
      if (((CompletableFuture) decodedRow).isDone()) {
        try {
          return (V) ((CompletableFuture) decodedRow).get();
        } catch (InterruptedException | ExecutionException e) {
          LOGGER.warn("unable to get results", e);
        }
      } else {
        outstandingFutures.incrementAndGet();
        ((CompletableFuture) decodedRow).whenComplete((decodedValue, exception) -> {
          if (decodedValue != null) {
            try {
              completedObjects.put(f.apply((V) decodedValue));
            } catch (final InterruptedException e) {
              LOGGER.error("Unable to put value in blocking queue", e);
            }
          } else if (exception != null) {
            LOGGER.error("Error decoding row", exception);
            scannedResultsExhausted.set(true);
            dataIndexRetrieval.notifyIteratorExhausted();
          }
          if ((outstandingFutures.decrementAndGet() == 0) && scannedResultsExhausted.get()) {
            try {
              completedObjects.put(POISON);
            } catch (final InterruptedException e) {
              LOGGER.error("Unable to put poison in blocking queue", e);
            }
          }
        });
      }
      return null;
    }
    return decodedRow;
  }

  public O postFindNext(final boolean hasNextValue, final boolean hasNextScannedResult) {
    if (!hasNextScannedResult && !scannedResultsExhausted.getAndSet(true)) {
      dataIndexRetrieval.notifyIteratorExhausted();
    }
    O retVal = null;
    if (!hasNextValue && ((outstandingFutures.get() > 0) || !completedObjects.isEmpty())) {
      try {
        final Object completedObj = completedObjects.take();
        if (completedObj == POISON) {
          retVal = null;
        } else {
          retVal = (O) completedObj;
        }
      } catch (final InterruptedException e) {
        LOGGER.error("Unable to take value from blocking queue", e);
      }
    }
    return retVal;
  }

  private static class NoOp implements Function<Object, Object> {

    @Override
    public Object apply(final Object t) {
      return t;
    }

  }
}
