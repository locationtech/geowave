/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import static org.junit.Assert.assertEquals;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;

public class RocksDBTableLifetimeTest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private RocksDBClient client;

  @Before
  public void createClient() {
    client = new RocksDBClient(folder.getRoot().getAbsolutePath(), false, false, 1000, false);
  }

  @After
  public void closeClients() {
    client.close();
    RocksDBClientCache.getInstance().closeAll();
  }

  @Test
  public void testTableReopensAfterTheSharedOptionsAreClosed() {
    final RocksDBIndexTable table = client.getIndexTable("table", (short) 0, null, false);
    add(table, 0, 500);
    client.close();
    // as closing the last store does, while this table is still held
    RocksDBClientCache.getInstance().closeAll();
    add(table, 500, 500);
    try (CloseableIterator<GeoWaveRow> rows = table.iterator()) {
      assertEquals(1000, Iterators.size(rows));
    }
  }

  private static void add(final RocksDBIndexTable table, final int start, final int count) {
    for (int i = start; i < (start + count); i++) {
      table.add(
          Ints.toByteArray(i),
          Ints.toByteArray(i),
          (short) 0,
          new GeoWaveValueImpl(new byte[0], new byte[0], Ints.toByteArray(i)));
    }
    table.flush();
  }
}
