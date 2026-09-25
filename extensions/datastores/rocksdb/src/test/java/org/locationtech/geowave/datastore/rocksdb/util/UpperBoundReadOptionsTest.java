/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

public class UpperBoundReadOptionsTest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testIterationStopsAtBoundAndCloseReleasesTheBound() throws RocksDBException {
    RocksDB.loadLibrary();
    final List<Byte> keys = new ArrayList<>();
    final Bound bound = new Bound(new byte[] {5});
    final UpperBoundReadOptions options = new UpperBoundReadOptions(bound);
    try (Options dbOptions = new Options().setCreateIfMissing(true);
        RocksDB db = RocksDB.open(dbOptions, folder.getRoot().getAbsolutePath())) {
      for (byte k = 1; k < 10; k++) {
        db.put(new byte[] {k}, new byte[] {k});
      }
      try (RocksIterator it = db.newIterator(options)) {
        for (it.seek(new byte[] {2}); it.isValid(); it.next()) {
          keys.add(it.key()[0]);
        }
      }
    } finally {
      options.close();
    }
    assertArrayEquals(new Byte[] {2, 3, 4}, keys.toArray(new Byte[0]));
    assertFalse(options.isOwningHandle());
    assertTrue(bound.isFreed());
  }

  private static class Bound extends Slice {
    private Bound(final byte[] data) {
      super(data);
    }

    private boolean isFreed() {
      return !isOwningHandle();
    }
  }
}
