/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb;

import org.junit.Test;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBMetadataTable;
import org.mockito.Mockito;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBMetadataTableTest {

  @Test
  public void testAddSimple() throws RocksDBException {
    final RocksDB db = Mockito.mock(RocksDB.class);
    final RocksDBMetadataTable metadataTable = new RocksDBMetadataTable(db, false, false, false);
    final byte[] primaryId = new byte[] {4};
    final byte[] secondaryId = new byte[] {2};
    final byte[] value = new byte[] {123};
    final GeoWaveMetadata metadata = new GeoWaveMetadata(primaryId, secondaryId, null, value);
    metadataTable.add(metadata);
    Mockito.verify(db).put(new byte[] {4, 2, 1}, value);
  }

  @Test
  public void testAddWithVisibility() throws RocksDBException {
    final RocksDB db = Mockito.mock(RocksDB.class);
    final RocksDBMetadataTable metadataTable = new RocksDBMetadataTable(db, false, true, false);
    final byte[] primaryId = new byte[] {4};
    final byte[] secondaryId = new byte[] {2};
    final byte[] value = new byte[] {123};
    final GeoWaveMetadata metadata1 = new GeoWaveMetadata(primaryId, secondaryId, null, value);
    metadataTable.add(metadata1);
    Mockito.verify(db).put(new byte[] {4, 2, 0, 1}, value);

    final byte[] visibility = new byte[] {6};
    final GeoWaveMetadata metadata2 =
        new GeoWaveMetadata(primaryId, secondaryId, visibility, value);
    metadataTable.add(metadata2);
    Mockito.verify(db).put(new byte[] {4, 2, 6, 1, 1}, value);
  }

  @Test
  public void testRemove() throws RocksDBException {
    final RocksDB db = Mockito.mock(RocksDB.class);
    final RocksDBMetadataTable metadataTable = new RocksDBMetadataTable(db, false, true, false);
    final byte[] keyToRemove = new byte[] {1, 2, 3};
    metadataTable.remove(keyToRemove);
    Mockito.verify(db).singleDelete(keyToRemove);
  }

}
