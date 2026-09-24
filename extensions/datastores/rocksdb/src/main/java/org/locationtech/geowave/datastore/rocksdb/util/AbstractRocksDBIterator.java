/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import java.util.NoSuchElementException;
import java.util.Set;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Methods are synchronized because the table can close this iterator from another thread when the
 * table itself is closed.
 */
public abstract class AbstractRocksDBIterator<T> implements CloseableIterator<T> {
  protected boolean closed = false;
  protected ReadOptions options;
  protected RocksIterator it;
  private Set<AbstractRocksDBIterator<?>> openIterators;
  private boolean closedWithTable = false;

  public AbstractRocksDBIterator(final ReadOptions options, final RocksIterator it) {
    super();
    this.options = options;
    this.it = it;
  }

  synchronized void trackIn(final Set<AbstractRocksDBIterator<?>> openIterators) {
    this.openIterators = openIterators;
    openIterators.add(this);
  }

  @Override
  public synchronized boolean hasNext() {
    checkTableOpen();
    if (closed) {
      return false;
    }
    if (it.isValid()) {
      return true;
    }
    // a read error also invalidates the iterator, which would otherwise look like the end of the
    // results
    try {
      it.status();
    } catch (final RocksDBException e) {
      throw new IllegalStateException("RocksDB iterator failed", e);
    }
    return false;
  }

  @Override
  public synchronized T next() {
    checkTableOpen();
    if (closed) {
      throw new NoSuchElementException();
    }
    final T retVal = readRow(it.key(), it.value());

    advance();
    return retVal;
  }

  private void checkTableOpen() {
    if (closedWithTable) {
      // ending the scan quietly here would silently truncate the results
      throw new IllegalStateException(
          "The RocksDB table was closed while this iterator was still open");
    }
  }

  protected void advance() {
    it.next();
  }

  protected abstract T readRow(byte[] key, byte[] value);

  @Override
  public synchronized void close() {
    if (openIterators != null) {
      openIterators.remove(this);
      openIterators = null;
    }
    release();
  }

  synchronized void closeWithTable() {
    if (!closed) {
      closedWithTable = true;
      openIterators = null;
      release();
    }
  }

  private void release() {
    closed = true;
    if (it != null) {
      it.close();
      it = null;
    }
    if (options != null) {
      options.close();
      options = null;
    }
  }
}
