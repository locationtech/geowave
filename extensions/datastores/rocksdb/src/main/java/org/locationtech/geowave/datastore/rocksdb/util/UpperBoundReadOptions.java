/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import org.rocksdb.ReadOptions;
import org.rocksdb.Slice;

/**
 * Read options that own their iterate upper bound. ReadOptions only keeps a reference to the bound,
 * and since RocksJava 7 nothing frees native memory on garbage collection, so the bound has to be
 * closed explicitly along with the options.
 */
class UpperBoundReadOptions extends ReadOptions {
  private final Slice upperBound;

  UpperBoundReadOptions(final byte[] upperBound) {
    this(new Slice(upperBound));
  }

  UpperBoundReadOptions(final Slice upperBound) {
    this.upperBound = upperBound;
    setIterateUpperBound(upperBound);
  }

  @Override
  public void close() {
    super.close();
    upperBound.close();
  }
}
