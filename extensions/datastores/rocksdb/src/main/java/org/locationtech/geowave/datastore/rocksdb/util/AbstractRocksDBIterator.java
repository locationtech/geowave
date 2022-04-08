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
import org.locationtech.geowave.core.store.CloseableIterator;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;

public abstract class AbstractRocksDBIterator<T> implements CloseableIterator<T> {
  protected boolean closed = false;
  protected ReadOptions options;
  protected RocksIterator it;

  public AbstractRocksDBIterator(final ReadOptions options, final RocksIterator it) {
    super();
    this.options = options;
    this.it = it;
  }

  @Override
  public boolean hasNext() {
    return !closed && it.isValid();
  }

  @Override
  public T next() {
    if (closed) {
      throw new NoSuchElementException();
    }
    final T retVal = readRow(it.key(), it.value());

    advance();
    return retVal;
  }

  protected void advance() {
    it.next();
  }

  protected abstract T readRow(byte[] key, byte[] value);

  @Override
  public void close() {
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
