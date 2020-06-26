package org.locationtech.geowave.test;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.locationtech.geowave.core.index.SPIServiceRegistry;
import org.locationtech.geowave.test.kerberos.KerberosTestingUtilSpi;
import java.util.Iterator;

public class KerberosTestEnvironment implements TestEnvironment {

  private static KerberosTestEnvironment singletonInstance = null;

  public static synchronized KerberosTestEnvironment getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new KerberosTestEnvironment();
    }
    return singletonInstance;
  }

  private KerberosTestingUtilSpi util;

  private KerberosTestEnvironment() {
    // merely because older versions of accumulo and versions with cloudera do not include
    // TestingKdc for compile-time compliance we use SPI to bring these dependencies in
    final Iterator<KerberosTestingUtilSpi> kerberosTestingUtilRegistry =
        new SPIServiceRegistry(KerberosTestEnvironment.class).load(KerberosTestingUtilSpi.class);
    while (kerberosTestingUtilRegistry.hasNext()) {
      final KerberosTestingUtilSpi kereberosTestingUtil = kerberosTestingUtilRegistry.next();
      if (kereberosTestingUtil != null) {
        util = kereberosTestingUtil;
      }
    }
  }

  @Override
  public void setup() throws Exception {
    util.setup();
  }

  @Override
  public void tearDown() throws Exception {
    util.tearDown();
  }

  @Override
  public TestEnvironment[] getDependentEnvironments() {
    return new TestEnvironment[0];
  }

  public void configureMiniAccumulo(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    util.configureMiniAccumulo(cfg, coreSite);
  }

  public ClusterUser getRootUser() {
    return util.getRootUser();
  }
}
