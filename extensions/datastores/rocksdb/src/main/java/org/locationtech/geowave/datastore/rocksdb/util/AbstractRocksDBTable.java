/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.util.concurrent.MoreExecutors;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

abstract public class AbstractRocksDBTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRocksDBTable.class);
  private static final int BATCH_WRITE_THREAD_SIZE = 16;
  private static final ExecutorService BATCH_WRITE_THREADS =
      MoreExecutors.getExitingExecutorService(
          (ThreadPoolExecutor) Executors.newFixedThreadPool(BATCH_WRITE_THREAD_SIZE));
  private static final int MAX_CONCURRENT_WRITE = 100;
  // only allow so many outstanding async reads or writes, use this semaphore
  // to control it
  private final Object BATCH_WRITE_MUTEX = new Object();
  private final Semaphore writeSemaphore = new Semaphore(MAX_CONCURRENT_WRITE);

  private WriteBatch currentBatch;
  private final int batchSize;
  private final ManagedRocksDB db;
  private final boolean walOnBatchWrite;
  protected final String subDirectory;
  protected final short adapterId;
  protected boolean visibilityEnabled;
  protected boolean compactOnWrite;
  private final boolean batchWrite;

  public AbstractRocksDBTable(
      final String subDirectory,
      final short adapterId,
      final boolean visibilityEnabled,
      final boolean compactOnWrite,
      final int batchSize,
      final boolean walOnBatchWrite) {
    super();
    // write batches are created before the database is opened
    RocksDB.loadLibrary();
    this.subDirectory = subDirectory;
    this.adapterId = adapterId;
    db = new ManagedRocksDB(subDirectory, RocksDBClient::openIndexDb);
    this.visibilityEnabled = visibilityEnabled;
    this.compactOnWrite = compactOnWrite;
    this.batchSize = batchSize;
    this.walOnBatchWrite = walOnBatchWrite;
    batchWrite = batchSize > 1;
  }

  protected ManagedRocksDB getManagedDb() {
    return db;
  }

  protected <T> CloseableIterator<T> iterator(
      final Supplier<ReadOptions> options,
      final BiFunction<ReadOptions, RocksIterator, AbstractRocksDBIterator<T>> seekAndWrap) {
    return db.iterator(options, seekAndWrap);
  }

  public void delete(final byte[] key) {
    try {
      final boolean exists = db.read(rocks -> {
        rocks.singleDelete(key);
        return true;
      }, () -> false);
      if (!exists) {
        LOGGER.warn("Unable to delete key because directory '" + subDirectory + "' doesn't exist");
      }
    } catch (final RocksDBException e) {
      LOGGER.warn("Unable to delete key", e);
    }
  }

  @SuppressFBWarnings(
      justification = "The null check outside of the synchronized block is intentional to minimize the need for synchronization.")
  protected void put(final byte[] key, final byte[] value) {
    if (batchWrite) {
      WriteBatch thisBatch = currentBatch;
      if (thisBatch == null) {
        synchronized (BATCH_WRITE_MUTEX) {
          if (currentBatch == null) {
            currentBatch = new WriteBatch();
          }
          thisBatch = currentBatch;
        }
      }
      try {
        thisBatch.put(key, value);
      } catch (final RocksDBException e) {
        LOGGER.warn("Unable to add data to batched write", e);
      }
      if (thisBatch.count() >= batchSize) {
        synchronized (BATCH_WRITE_MUTEX) {
          if (currentBatch != null) {
            flushWriteQueue();
          }
        }
      }
    } else {
      try {
        db.write(rocks -> {
          rocks.put(key, value);
          return null;
        });
      } catch (final RocksDBException e) {
        LOGGER.warn("Unable to write key-value", e);
      }
    }
  }

  private void flushWriteQueue() {
    try {
      writeSemaphore.acquire();
      CompletableFuture.runAsync(
          new BatchWriter(currentBatch, db, walOnBatchWrite, writeSemaphore),
          BATCH_WRITE_THREADS);
    } catch (final InterruptedException e) {
      LOGGER.warn("async write semaphore interrupted", e);
      writeSemaphore.release();
    }
    currentBatch = null;
  }

  @SuppressFBWarnings(
      justification = "The null check outside of the synchronized block is intentional to minimize the need for synchronization.")
  public void flush() {
    if (batchWrite) {
      synchronized (BATCH_WRITE_MUTEX) {
        if (currentBatch != null) {
          flushWriteQueue();
        }
        waitForBatchWrite();
      }
    }
    internalFlush();
  }

  protected void internalFlush() {
    if (compactOnWrite) {
      try {
        db.read(rocks -> {
          rocks.compactRange();
          return null;
        }, () -> null);
      } catch (final RocksDBException e) {
        LOGGER.warn("Unable to compact range", e);
      }
    }
  }

  public void compact() {
    try {
      db.read(rocks -> {
        rocks.compactRange();
        return null;
      }, () -> null);
    } catch (final RocksDBException e) {
      LOGGER.warn("Unable to force compacting range", e);
    }
  }

  private void waitForBatchWrite() {
    if (batchWrite) {
      // need to wait for all asynchronous batches to finish writing
      // before exiting close() method
      try {
        writeSemaphore.acquire(MAX_CONCURRENT_WRITE);
      } catch (final InterruptedException e) {
        LOGGER.warn("Unable to wait for batch write to complete");
      }
      writeSemaphore.release(MAX_CONCURRENT_WRITE);
    }
  }

  boolean isOpen() {
    return db.isOpen();
  }

  /**
   * Closes the database and any iterators still open on it. The table stays usable, and reopens the
   * database when it is next used.
   */
  public void close() {
    waitForBatchWrite();
    db.close();
  }

  public String getSubDirectory() {
    return subDirectory;
  }

  private static class BatchWriter implements Runnable {
    private final WriteBatch dataToWrite;
    private final ManagedRocksDB db;
    private final boolean walOnBatchWrite;
    private final Semaphore writeSemaphore;

    private BatchWriter(
        final WriteBatch dataToWrite,
        final ManagedRocksDB db,
        final boolean walOnBatchWrite,
        final Semaphore writeSemaphore) {
      super();
      this.dataToWrite = dataToWrite;
      this.db = db;
      this.walOnBatchWrite = walOnBatchWrite;
      this.writeSemaphore = writeSemaphore;
    }

    @Override
    public void run() {
      // the write options are this write's own, so nothing else can close them while it runs
      try (WriteOptions options = new WriteOptions().setDisableWAL(!walOnBatchWrite)) {
        db.write(rocks -> {
          rocks.write(options, dataToWrite);
          return null;
        });
        dataToWrite.close();
      } catch (final RocksDBException e) {
        LOGGER.warn("Unable to write batch", e);
      } finally {
        writeSemaphore.release();
      }
    }
  }
}
