/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import java.util.NoSuchElementException;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.rocksdb.RocksIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.UnsignedBytes;

public class DataIndexBoundedReverseRowIterator implements CloseableIterator<GeoWaveRow> {
  private final DataIndexReverseRowIterator delegate;
  private final PeekingIterator<GeoWaveRow> peekingIterator;
  private final byte[] startDataId;
  boolean hasNext = true;

  public DataIndexBoundedReverseRowIterator(
      final byte[] startDataId,
      final RocksIterator it,
      final short adapterId,
      final boolean visiblityEnabled) {
    delegate = new DataIndexReverseRowIterator(it, adapterId, visiblityEnabled);
    this.startDataId = startDataId;
    // because there is no RocksDB option to set a lower bound this needs to be a peeking iterator
    // to check for passing the start data ID
    peekingIterator = Iterators.peekingIterator(delegate);
  }

  @Override
  public boolean hasNext() {
    if (!delegate.closed
        && peekingIterator.hasNext()
        && (UnsignedBytes.lexicographicalComparator().compare(
            startDataId,
            peekingIterator.peek().getDataId()) <= 0)) {
      return true;
    }
    hasNext = false;
    return false;
  }

  @Override
  public GeoWaveRow next() {
    if (!hasNext) {
      throw new NoSuchElementException();
    }
    return peekingIterator.next();
  }

  @Override
  public void close() {
    delegate.close();
    hasNext = false;
  }

}
