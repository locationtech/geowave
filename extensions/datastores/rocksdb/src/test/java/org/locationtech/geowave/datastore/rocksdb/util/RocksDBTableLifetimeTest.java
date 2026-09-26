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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.rocksdb.RocksDBDataStore;
import org.locationtech.geowave.datastore.rocksdb.config.RocksDBOptions;
import org.locationtech.geowave.datastore.rocksdb.operations.RocksDBOperations;
import org.rocksdb.RocksIterator;
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

  @Test
  public void testClosingTablesClosesIteratorsStillOpenOnThem() {
    final RocksDBIndexTable table = client.getIndexTable("table", (short) 0, null, false);
    add(table, 0, 500);
    final RocksDBMetadataTable metadataTable = client.getMetadataTable(MetadataType.ADAPTER);
    metadataTable.add(new GeoWaveMetadata(new byte[] {1}, new byte[] {2}, null, new byte[] {3}));

    final CloseableIterator<GeoWaveRow> rows = table.iterator();
    rows.next();
    final CloseableIterator<GeoWaveMetadata> metadata = metadataTable.iterator();
    final RocksIterator nativeRows = ((AbstractRocksDBIterator<?>) rows).it;
    final RocksIterator nativeMetadata = ((AbstractRocksDBIterator<?>) metadata).it;
    client.close();

    assertFalse(nativeRows.isOwningHandle());
    assertFalse(nativeMetadata.isOwningHandle());
    assertThrows(IllegalStateException.class, rows::hasNext);
    assertThrows(IllegalStateException.class, metadata::next);
    rows.close();
    metadata.close();

    try (CloseableIterator<GeoWaveRow> reopened = table.iterator()) {
      assertEquals(500, Iterators.size(reopened));
    }
  }

  @Test
  public void testDatabasesCloseWhenTheLastStoreOnTheDirectoryCloses() {
    final RocksDBOptions options = new RocksDBOptions();
    options.setDirectory(folder.getRoot().getAbsolutePath());
    final RocksDBOperations operations = new RocksDBOperations(options);
    final RocksDBDataStore store = new RocksDBDataStore(operations, options.getStoreOptions());
    final RocksDBDataStore other =
        new RocksDBDataStore(new RocksDBOperations(options), options.getStoreOptions());
    final RocksDBIndexTable table =
        operations.getClient().getIndexTable("table", (short) 0, null, false);
    add(table, 0, 500);
    final CloseableIterator<GeoWaveRow> rows = table.iterator();
    rows.next();

    other.close();
    assertTrue(table.isOpen());
    rows.next();

    store.close();
    assertFalse(table.isOpen());
    assertThrows(IllegalStateException.class, rows::hasNext);
    rows.close();
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
