package org.locationtech.geowave.datastore.rocksdb;

import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;
import org.locationtech.geowave.datastore.rocksdb.RocksDBLockfileTest.POIBasicDataAdapter;

public class RocksDBTestPersistableRegistry implements PersistableRegistrySpi {

  @Override
  public PersistableIdAndConstructor[] getSupportedPersistables() {
    return new PersistableIdAndConstructor[] {
        new PersistableIdAndConstructor((short) 20050, POIBasicDataAdapter::new)};
  }
}
