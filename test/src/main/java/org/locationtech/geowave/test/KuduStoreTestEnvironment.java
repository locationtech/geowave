package org.locationtech.geowave.test;

import org.locationtech.geowave.core.store.GenericStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.datastore.kudu.KuduStoreFactoryFamily;
import org.locationtech.geowave.datastore.kudu.config.KuduRequiredOptions;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduStoreTestEnvironment extends StoreTestEnvironment {

  private static final Logger LOGGER = LoggerFactory.getLogger(KuduStoreTestEnvironment.class);
  private static final GenericStoreFactory<DataStore> STORE_FACTORY =
      new KuduStoreFactoryFamily().getDataStoreFactory();

  private static KuduStoreTestEnvironment singletonInstance = null;

  public static synchronized KuduStoreTestEnvironment getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new KuduStoreTestEnvironment();
    }
    return singletonInstance;
  }

  private final KuduLocal kuduLocal;

  private KuduStoreTestEnvironment() {
    kuduLocal = new KuduLocal(null, 1);
  }

  @Override
  public void setup() throws Exception {
    // Make sure we clean up any old processes first
    if (kuduLocal.isRunning()) {
      kuduLocal.stop();
    }

    if (!kuduLocal.start()) {
      LOGGER.error("Kudu database startup failed");
    }
  }

  @Override
  public void tearDown() throws Exception {
    kuduLocal.stop();
    kuduLocal.destroyDB();
  }

  @Override
  public TestEnvironment[] getDependentEnvironments() {
    return new TestEnvironment[] {};
  }

  @Override
  protected GenericStoreFactory<DataStore> getDataStoreFactory() {
    return STORE_FACTORY;
  }

  @Override
  protected GeoWaveStoreType getStoreType() {
    return GeoWaveStoreType.KUDU;
  }

  @Override
  protected void initOptions(StoreFactoryOptions options) {
    KuduRequiredOptions kuduOptions = (KuduRequiredOptions) options;
    kuduOptions.setKuduMaster("127.0.0.1:7051");
  }

  @Override
  public int getMaxCellSize() {
    // https://www.cloudera.com/documentation/enterprise/latest/topics/kudu_limitations.html#schema_design_limitations
    return 64 * 1024;
  }

}
