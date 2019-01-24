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
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

abstract public class AbstractRocksDBTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRocksDBTable.class);
  private RocksDB writeDb;
  private RocksDB readDb;
  private final Options writeOptions;
  private final Options readOptions;
  private final String subDirectory;
  private boolean readerDirty = false;
  private boolean exists;
  protected final short adapterId;
  protected boolean visibilityEnabled;

  public AbstractRocksDBTable(
      final Options writeOptions,
      final Options readOptions,
      final String subDirectory,
      final short adapterId,
      final boolean visibilityEnabled) {
    super();
    this.writeOptions = writeOptions;
    this.readOptions = readOptions;
    this.subDirectory = subDirectory;
    this.adapterId = adapterId;
    exists = new File(subDirectory).exists();
    this.visibilityEnabled = visibilityEnabled;
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
    // TODO batch writes
    final RocksDB db = getWriteDb();
    try {
      readerDirty = true;
      db.put(key, value);
    } catch (final RocksDBException e) {
      LOGGER.warn("Unable to write key-value", e);
    }
  }

  @SuppressFBWarnings(
      justification = "The null check outside of the synchronized block is intentional to minimize the need for synchronization.")
  public void flush() {
    // TODO flush batch writes
    final RocksDB db = getWriteDb();
    try {
      db.compactRange();
    } catch (final RocksDBException e) {
      LOGGER.warn("Unable to compact range", e);
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

  public void close() {
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
}
