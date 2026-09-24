/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import java.io.File;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The RocksDB instance behind one table directory. It opens on first use, and again after close(),
 * so that anything still holding the table keeps working after a store sharing it is closed.
 * close() waits for native calls that are in progress.
 */
final class ManagedRocksDB {
  @FunctionalInterface
  interface Opener {
    RocksDB open(String directory) throws RocksDBException;
  }

  @FunctionalInterface
  interface Operation<T> {
    T apply(RocksDB db) throws RocksDBException;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ManagedRocksDB.class);
  private final String directory;
  private final Opener opener;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private RocksDB db;

  ManagedRocksDB(final String directory, final Opener opener) {
    this(directory, opener, null);
  }

  /** A null opener means the database cannot be reopened once closed. */
  ManagedRocksDB(final String directory, final Opener opener, final RocksDB openDb) {
    this.directory = directory;
    this.opener = opener;
    db = openDb;
  }

  boolean isOpen() {
    lock.readLock().lock();
    try {
      return db != null;
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Runs the operation if the table exists, without creating it. */
  <T> T read(final Operation<T> operation, final Supplier<T> ifMissing) throws RocksDBException {
    return run(false, operation, ifMissing);
  }

  /** Runs the operation, creating the table if it does not exist yet. */
  <T> T write(final Operation<T> operation) throws RocksDBException {
    return run(true, operation, () -> {
      throw new IllegalStateException("Unable to create directory '" + directory + "'");
    });
  }

  /**
   * The read options are only created once the database is open, and seekAndWrap must hand them to
   * the iterator it returns, which closes them.
   */
  <T> CloseableIterator<T> iterator(
      final Supplier<ReadOptions> options,
      final BiFunction<ReadOptions, RocksIterator, AbstractRocksDBIterator<T>> seekAndWrap) {
    try {
      return read(db -> {
        final ReadOptions readOptions = options.get();
        final RocksIterator it =
            readOptions == null ? db.newIterator() : db.newIterator(readOptions);
        return seekAndWrap.apply(readOptions, it);
      }, CloseableIterator.Empty::new);
    } catch (final RocksDBException e) {
      LOGGER.error("Unable to open '" + directory + "' for reading", e);
      return new CloseableIterator.Empty<>();
    }
  }

  private <T> T run(final boolean create, final Operation<T> operation, final Supplier<T> ifMissing)
      throws RocksDBException {
    while (true) {
      lock.readLock().lock();
      try {
        if (db != null) {
          return operation.apply(db);
        }
      } finally {
        lock.readLock().unlock();
      }
      // a close() between opening and reacquiring the read lock just means opening again
      if (!open(create)) {
        return ifMissing.get();
      }
    }
  }

  private boolean open(final boolean create) throws RocksDBException {
    lock.writeLock().lock();
    try {
      if (db == null) {
        if (opener == null) {
          throw new IllegalStateException("RocksDB table '" + directory + "' is closed");
        }
        final File dir = new File(directory);
        if (!dir.isDirectory() && (!create || !(dir.mkdirs() || dir.isDirectory()))) {
          return false;
        }
        db = opener.open(directory);
      }
      return true;
    } finally {
      lock.writeLock().unlock();
    }
  }

  void close() {
    lock.writeLock().lock();
    try {
      if (db != null) {
        db.close();
        db = null;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }
}
