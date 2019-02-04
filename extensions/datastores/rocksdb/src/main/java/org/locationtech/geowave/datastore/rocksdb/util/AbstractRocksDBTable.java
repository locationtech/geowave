/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
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
  private RocksDB writeDb;
  private RocksDB readDb;
  private final Options writeOptions;
  private final Options readOptions;
  private final WriteOptions batchWriteOptions;
  private final String subDirectory;
  private boolean readerDirty = false;
  private boolean exists;
  protected final short adapterId;
  protected boolean visibilityEnabled;
  protected boolean compactOnWrite;
  private final boolean batchWrite;

  public AbstractRocksDBTable(
      final Options writeOptions,
      final Options readOptions,
      final WriteOptions batchWriteOptions,
      final String subDirectory,
      final short adapterId,
      final boolean visibilityEnabled,
      final boolean compactOnWrite,
      final int batchSize) {
    super();
    this.writeOptions = writeOptions;
    this.readOptions = readOptions;
    this.batchWriteOptions = batchWriteOptions;
    this.subDirectory = subDirectory;
    this.adapterId = adapterId;
    exists = new File(subDirectory).exists();
    this.visibilityEnabled = visibilityEnabled;
    this.compactOnWrite = compactOnWrite;
    this.batchSize = batchSize;
    batchWrite = batchSize > 1;
  }

  public synchronized void delete(final byte[] key) {
    final RocksDB db = getWriteDb();
    try {
      readerDirty = true;
      db.singleDelete(key);
    } catch (final RocksDBException e) {
      LOGGER.warn("Unable to delete key", e);
    }
  }

  protected synchronized void put(final byte[] key, final byte[] value) {
    if (batchWrite) {
      synchronized (BATCH_WRITE_MUTEX) {
        if (currentBatch == null) {
          currentBatch = new WriteBatch();
        }
        try {
          currentBatch.put(key, value);
        } catch (final RocksDBException e) {
          LOGGER.warn("Unable to add data to batched write", e);
        }
        if (currentBatch.count() >= batchSize) {
          flushWriteQueue();
        }
      }
    } else {
      final RocksDB db = getWriteDb();
      try {
        readerDirty = true;
        db.put(key, value);
      } catch (final RocksDBException e) {
        LOGGER.warn("Unable to write key-value", e);
      }
    }
  }

  private void flushWriteQueue() {
    try {
      writeSemaphore.acquire();
      readerDirty = true;
      CompletableFuture.runAsync(
          new BatchWriter(currentBatch, getWriteDb(), batchWriteOptions, writeSemaphore),
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
    if (compactOnWrite) {
      final RocksDB db = getWriteDb();
      try {
        db.compactRange();
      } catch (final RocksDBException e) {
        LOGGER.warn("Unable to compact range", e);
      }
    }
    // force re-opening a reader to catch the updates from this write
    if (readerDirty && (readDb != null)) {
      synchronized (this) {
        if (readDb != null) {
          readDb.close();
          readDb = null;
        }
      }
    }
  }

  public void compact() {
    final RocksDB db = getWriteDb();
    try {
      db.compactRange();
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

  public void close() {
    waitForBatchWrite();
    synchronized (this) {
      if (writeDb != null) {
        writeDb.close();
        writeDb = null;
      }
      if (readDb != null) {
        readDb.close();
      }
    }
  }

  @SuppressFBWarnings(
      justification = "double check for null is intentional to avoid synchronized blocks when not needed.")
  protected RocksDB getWriteDb() {
    // avoid synchronization if unnecessary by checking for null outside
    // synchronized block
    if (writeDb == null) {
      synchronized (this) {
        // check again within synchronized block
        if (writeDb == null) {
          try {
            if (exists || new File(subDirectory).mkdirs()) {
              exists = true;
              writeDb = RocksDB.open(writeOptions, subDirectory);
            } else {
              LOGGER.error("Unable to open to create directory '" + subDirectory + "'");
            }
          } catch (final RocksDBException e) {
            LOGGER.error("Unable to open for writing", e);
          }
        }
      }
    }
    return writeDb;
  }

  @SuppressFBWarnings(
      justification = "double check for null is intentional to avoid synchronized blocks when not needed.")
  protected RocksDB getReadDb() {
    if (!exists) {
      return null;
    }
    // avoid synchronization if unnecessary by checking for null outside
    // synchronized block
    if (readDb == null) {
      synchronized (this) {
        // check again within synchronized block
        if (readDb == null) {
          try {
            readerDirty = false;
            readDb = RocksDB.openReadOnly(readOptions, subDirectory);
          } catch (final RocksDBException e) {
            LOGGER.warn("Unable to open for reading", e);
          }
        }
      }
    }
    return readDb;
  }

  private static class BatchWriter implements Runnable {
    private final WriteBatch dataToWrite;
    private final RocksDB db;
    private final WriteOptions options;
    private final Semaphore writeSemaphore;

    private BatchWriter(
        final WriteBatch dataToWrite,
        final RocksDB db,
        final WriteOptions options,
        final Semaphore writeSemaphore) {
      super();
      this.dataToWrite = dataToWrite;
      this.db = db;
      this.options = options;
      this.writeSemaphore = writeSemaphore;
    }

    @Override
    public void run() {
      try {
        db.write(options, dataToWrite);
        dataToWrite.close();
      } catch (final RocksDBException e) {
        LOGGER.warn("Unable to write batch", e);
      } finally {
        writeSemaphore.release();
      }
    }
  }
}
