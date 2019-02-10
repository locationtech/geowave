package org.locationtech.geowave.datastore.rocksdb.util;

import org.junit.Test;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.mockito.Mockito;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBMetadataTableTest {

    @Test
    public void testAddSimple() throws RocksDBException {
	RocksDB db = Mockito.mock(RocksDB.class);
	RocksDBMetadataTable metadataTable = new RocksDBMetadataTable(db, false, false, false);
	byte[] primaryId = new byte[] {4};
	byte[] secondaryId = new byte[] {2};
	byte[] value = new byte[] {123};
	GeoWaveMetadata metadata = new GeoWaveMetadata(primaryId, secondaryId, null, value);
	metadataTable.add(metadata);
	Mockito.verify(db).put(new byte[] {4, 2, 1}, value);
    }

    @Test
    public void testAddWithVisibility() throws RocksDBException {
	RocksDB db = Mockito.mock(RocksDB.class);
	RocksDBMetadataTable metadataTable = new RocksDBMetadataTable(db, false, true, false);
	byte[] primaryId = new byte[] {4};
	byte[] secondaryId = new byte[] {2};
	byte[] value = new byte[] {123};
	GeoWaveMetadata metadata1 = new GeoWaveMetadata(primaryId, secondaryId, null, value);
	metadataTable.add(metadata1);
	Mockito.verify(db).put(new byte[] {4, 2, 0, 1}, value);

	byte[] visibility = new byte[] {6};
	GeoWaveMetadata metadata2 = new GeoWaveMetadata(primaryId, secondaryId, visibility, value);
	metadataTable.add(metadata2);
	Mockito.verify(db).put(new byte[] {4, 2, 6, 1, 1}, value);
    }

    @Test
    public void testRemove() throws RocksDBException {
	RocksDB db = Mockito.mock(RocksDB.class);
	RocksDBMetadataTable metadataTable = new RocksDBMetadataTable(db, false, true, false);
	byte[] keyToRemove = new byte[] {1, 2, 3};
	metadataTable.remove(keyToRemove);
	Mockito.verify(db).singleDelete(keyToRemove);
    }

}
