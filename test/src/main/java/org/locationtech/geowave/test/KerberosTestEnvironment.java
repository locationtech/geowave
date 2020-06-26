/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.locationtech.geowave.core.index.SPIServiceRegistry;
import org.locationtech.geowave.test.kerberos.KerberosTestingUtilSpi;
import java.util.Iterator;

public class KerberosTestEnvironment implements TestEnvironment {

  private static KerberosTestEnvironment singletonInstance = null;

  private static final String TEST_KERBEROS_ENVIRONMENT_VARIABLE_NAME = "TEST_KERBEROS";
  private static final String TEST_KERBEROS_PROPERTY_NAME = "testKerberos";

  public static synchronized KerberosTestEnvironment getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new KerberosTestEnvironment();
    }
    return singletonInstance;
  }

  private KerberosTestingUtilSpi util;

  private KerberosTestEnvironment() {

  }

  public boolean isRunning() {
    return util != null;
  }

  public static boolean useKerberos() {
    String kerberosStr = System.getenv(TEST_KERBEROS_ENVIRONMENT_VARIABLE_NAME);
    if (!TestUtils.isSet(kerberosStr)) {
      kerberosStr = System.getProperty(TEST_KERBEROS_PROPERTY_NAME);
    }
    return TestUtils.isSet(kerberosStr) && "true".equalsIgnoreCase(kerberosStr);
  }

  private KerberosTestingUtilSpi getUtil() {
    // merely because older versions of accumulo and versions with cloudera do not include
    // TestingKdc for compile-time compliance we use SPI to bring these dependencies in
    final Iterator<KerberosTestingUtilSpi> kerberosTestingUtilRegistry =
        new SPIServiceRegistry(KerberosTestEnvironment.class).load(KerberosTestingUtilSpi.class);
    while (kerberosTestingUtilRegistry.hasNext()) {
      final KerberosTestingUtilSpi kereberosTestingUtil = kerberosTestingUtilRegistry.next();
      if (kereberosTestingUtil != null) {
        return kereberosTestingUtil;
      }
    }
    return null;
  }

  @Override
  public void setup() throws Exception {
    if (util == null) {
      util = getUtil();
      util.setup();
    }
  }

  @Override
  public void tearDown() throws Exception {
    util.tearDown();
    util = null;
  }

  @Override
  public TestEnvironment[] getDependentEnvironments() {
    return new TestEnvironment[0];
  }

  public void configureMiniAccumulo(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    if (util != null) {
      util.configureMiniAccumulo(cfg, coreSite);
    }
  }

  public ClusterUser getRootUser() {
    if (util != null) {
      return util.getRootUser();
    }
    return null;
  }
}
