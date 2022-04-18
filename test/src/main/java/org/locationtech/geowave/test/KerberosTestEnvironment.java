/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.MiniClusterHarness;
import org.apache.accumulo.harness.TestingKdc;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.server.security.handler.KerberosAuthenticator;
import org.apache.accumulo.server.security.handler.KerberosAuthorizor;
import org.apache.accumulo.server.security.handler.KerberosPermissionHandler;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.locationtech.geowave.datastore.accumulo.cli.MiniAccumuloUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOGGER = LoggerFactory.getLogger(KerberosTestEnvironment.class);
  private TestingKdc kdc;

  protected static final File TEMP_DIR = new File("./target/kerberos_temp");
  protected static final File TEMP_KEYTABS_DIR = new File(TEMP_DIR, "keytabs");
  public static final String TRUE = Boolean.toString(true);
  // TODO These are defined in MiniKdc >= 2.6.0. Can be removed when minimum Hadoop dependency is
  // increased to that.
  public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf",
      SUN_SECURITY_KRB5_DEBUG = "sun.security.krb5.debug";
  private ClusterUser rootUser;
  private boolean running = false;


  private KerberosTestEnvironment() {

  }

  public boolean isRunning() {
    return running;
  }

  public static boolean useKerberos() {
    String kerberosStr = System.getenv(TEST_KERBEROS_ENVIRONMENT_VARIABLE_NAME);
    if (!TestUtils.isSet(kerberosStr)) {
      kerberosStr = System.getProperty(TEST_KERBEROS_PROPERTY_NAME);
    }
    return TestUtils.isSet(kerberosStr) && "true".equalsIgnoreCase(kerberosStr);
  }

  @Override
  public void setup() throws Exception {
    Assert.assertTrue(TEMP_DIR.mkdirs() || TEMP_DIR.isDirectory());
    Assert.assertTrue(TEMP_KEYTABS_DIR.mkdirs() || TEMP_KEYTABS_DIR.isDirectory());
    kdc = new TestingKdc(TEMP_DIR, TEMP_KEYTABS_DIR);
    kdc.start();
    System.setProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION, "true");
    rootUser = kdc.getRootUser();
    // Enabled kerberos auth
    final Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    running = true;
  }

  @Override
  public void tearDown() throws Exception {
    if (kdc != null) {
      kdc.stop();
      running = false;
    }
    if (TEMP_DIR.exists()) {
      try {
        // sleep because mini accumulo processes still have a
        // hold on the log files and there is no hook to get
        // notified when it is completely stopped

        Thread.sleep(2000);
        FileUtils.deleteDirectory(TEMP_DIR);
      } catch (final IOException | InterruptedException e) {
        LOGGER.warn("Unable to delete mini Kerberos temporary directory", e);
      }
    }
    UserGroupInformation.setConfiguration(new Configuration(false));
  }

  @Override
  public TestEnvironment[] getDependentEnvironments() {
    return new TestEnvironment[0];
  }

  public void configureMiniAccumulo(final MiniAccumuloConfig cfg, final Configuration coreSite) {
    // Disable native maps by default
    MiniAccumuloUtils.setProperty(cfg, Property.TSERV_NATIVEMAP_ENABLED, Boolean.FALSE.toString());
    final Map<String, String> siteConfig = cfg.getSiteConfig();
    if (TRUE.equals(siteConfig.get(Property.INSTANCE_RPC_SSL_ENABLED.getKey()))) {
      throw new RuntimeException("Cannot use both SSL and SASL/Kerberos");
    }

    if (TRUE.equals(siteConfig.get(Property.INSTANCE_RPC_SASL_ENABLED.getKey()))) {
      // already enabled
      return;
    }

    if (kdc == null) {
      throw new IllegalStateException("MiniClusterKdc was null");
    }

    LOGGER.info("Enabling Kerberos/SASL for minicluster");

    // Turn on SASL and set the keytab/principal information

    MiniAccumuloUtils.setProperty(cfg, Property.INSTANCE_RPC_SASL_ENABLED, "true");
    final ClusterUser serverUser = kdc.getAccumuloServerUser();
    MiniAccumuloUtils.setProperty(
        cfg,
        Property.GENERAL_KERBEROS_KEYTAB,
        serverUser.getKeytab().getAbsolutePath());
    MiniAccumuloUtils.setProperty(
        cfg,
        Property.GENERAL_KERBEROS_PRINCIPAL,
        serverUser.getPrincipal());
    MiniAccumuloUtils.setProperty(
        cfg,
        Property.INSTANCE_SECURITY_AUTHENTICATOR,
        KerberosAuthenticator.class.getName());
    MiniAccumuloUtils.setProperty(
        cfg,
        Property.INSTANCE_SECURITY_AUTHORIZOR,
        KerberosAuthorizor.class.getName());
    MiniAccumuloUtils.setProperty(
        cfg,
        Property.INSTANCE_SECURITY_PERMISSION_HANDLER,
        KerberosPermissionHandler.class.getName());
    // Piggy-back on the "system user" credential, but use it as a normal KerberosToken, not the
    // SystemToken.
    MiniAccumuloUtils.setProperty(cfg, Property.TRACE_USER, serverUser.getPrincipal());
    MiniAccumuloUtils.setProperty(cfg, Property.TRACE_TOKEN_TYPE, KerberosToken.CLASS_NAME);
    // Pass down some KRB5 debug properties
    final Map<String, String> systemProperties = MiniAccumuloUtils.getSystemProperties(cfg);
    systemProperties.put(JAVA_SECURITY_KRB5_CONF, System.getProperty(JAVA_SECURITY_KRB5_CONF, ""));
    systemProperties.put(
        SUN_SECURITY_KRB5_DEBUG,
        System.getProperty(SUN_SECURITY_KRB5_DEBUG, "false"));
    MiniAccumuloUtils.setSystemProperties(cfg, systemProperties);
    MiniAccumuloUtils.setRootUserName(cfg, kdc.getRootUser().getPrincipal());
    MiniAccumuloUtils.setClientProperty(
        cfg,
        MiniAccumuloUtils.getClientProperty("SASL_ENABLED"),
        "true");
    // Make sure UserGroupInformation will do the correct login
    coreSite.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
  }

  public ClusterUser getRootUser() {
    return rootUser;
  }
}
