package org.locationtech.geowave.datastore.foundationdb;

import org.locationtech.geowave.core.store.StoreFactoryHelper;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.datastore.foundationdb.config.FoundationDBOptions;
import org.locationtech.geowave.datastore.foundationdb.operations.FoundationDBOperations;

public class FoundationDBFactoryHelper implements StoreFactoryHelper {
  @Override
  public StoreFactoryOptions createOptionsInstance() {
    return new FoundationDBOptions();
  }

  @Override
  public DataStoreOperations createOperations(final StoreFactoryOptions options) {
    return new FoundationDBOperations((FoundationDBOptions) options);
  }
}
