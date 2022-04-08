/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.util;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import org.redisson.api.BatchOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

abstract public class AbstractRedisSetWrapper<A, S> implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRedisSetWrapper.class);
  private static int BATCH_SIZE = 1000;
  private A currentAsync;
  private S currentSync;
  private RBatch currentBatch;
  private final RedissonClient client;
  private final String setName;
  private final Codec codec;
  private int batchCmdCounter = 0;
  private static final int MAX_CONCURRENT_WRITE = 100;
  private final Semaphore writeSemaphore = new Semaphore(MAX_CONCURRENT_WRITE);

  public AbstractRedisSetWrapper(
      final RedissonClient client,
      final String setName,
      final Codec codec) {
    this.setName = setName;
    this.client = client;
    this.codec = codec;
  }

  public void flush() {
    batchCmdCounter = 0;
    final RBatch flushBatch = this.currentBatch;
    currentAsync = null;
    currentBatch = null;
    if (flushBatch == null) {
      return;
    }
    try {
      writeSemaphore.acquire();
      flushBatch.executeAsync().handle((r, t) -> {
        writeSemaphore.release();
        if ((t != null) && !(t instanceof CancellationException)) {
          LOGGER.error("Exception in batched write", t);
        }
        return r;
      });
    } catch (final InterruptedException e) {
      LOGGER.warn("async batch write semaphore interrupted", e);
      writeSemaphore.release();
    }
  }

  @SuppressFBWarnings(justification = "This is intentional to avoid unnecessary sync")
  protected S getCurrentSyncCollection() {
    // avoid synchronization if unnecessary by checking for null outside
    // synchronized block
    if (currentSync == null) {
      synchronized (this) {
        // check again within synchronized block
        if (currentSync == null) {
          currentSync = initSyncCollection(client, setName, codec);
        }
      }
    }
    return currentSync;
  }

  @SuppressFBWarnings(justification = "This is intentional to avoid unnecessary sync")
  protected A getCurrentAsyncCollection() {
    // avoid synchronization if unnecessary by checking for null outside
    // synchronized block
    if (currentAsync == null) {
      synchronized (this) {
        // check again within synchronized block
        if (currentAsync == null) {
          currentBatch = client.createBatch(BatchOptions.defaults());
          currentAsync = initAsyncCollection(currentBatch, setName, codec);
        }
      }
    }
    return currentAsync;
  }

  abstract protected A initAsyncCollection(RBatch batch, String setName, Codec codec);

  abstract protected S initSyncCollection(RedissonClient client, String setName, Codec codec);

  protected void preAdd() {
    if (++batchCmdCounter > BATCH_SIZE) {
      synchronized (this) {
        // check again inside the synchronized block
        if (batchCmdCounter > BATCH_SIZE) {
          flush();
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    flush();
    // need to wait for all asynchronous batches to finish writing
    // before exiting close() method
    writeSemaphore.acquire(MAX_CONCURRENT_WRITE);
    writeSemaphore.release(MAX_CONCURRENT_WRITE);
  }
}
