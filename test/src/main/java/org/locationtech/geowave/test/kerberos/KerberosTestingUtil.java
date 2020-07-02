/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.kerberos;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.MiniClusterHarness;
import org.apache.accumulo.harness.TestingKdc;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.security.handler.KerberosAuthenticator;
import org.apache.accumulo.server.security.handler.KerberosAuthorizor;
import org.apache.accumulo.server.security.handler.KerberosPermissionHandler;
import org.apache.accumulo.test.functional.NativeMapIT;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.Map;

public class KerberosTestingUtil implements KerberosTestingUtilSpi {
  private static final Logger LOGGER = LoggerFactory.getLogger(KerberosTestingUtil.class);
  private TestingKdc kdc;

  protected static final File TEMP_DIR = new File("./target/kerberos_temp");
  protected static final File TEMP_KEYTABS_DIR = new File(TEMP_DIR, "keytabs");
  public static final String TRUE = Boolean.toString(true);
  // TODO These are defined in MiniKdc >= 2.6.0. Can be removed when minimum Hadoop dependency is
  // increased to that.
  public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf",
      SUN_SECURITY_KRB5_DEBUG = "sun.security.krb5.debug";
  private ClusterUser rootUser;

  public void setup() throws Exception {

    Assert.assertTrue(TEMP_DIR.mkdirs() || TEMP_DIR.isDirectory());
    Assert.assertTrue(TEMP_KEYTABS_DIR.mkdirs() || TEMP_KEYTABS_DIR.isDirectory());
    kdc = new TestingKdc(TEMP_DIR, TEMP_KEYTABS_DIR);
    kdc.start();
    System.setProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION, "true");
    rootUser = kdc.getRootUser();
    // Enabled kerberos auth
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
  }

  public void tearDown() throws Exception {
    if (null != kdc) {
      kdc.stop();
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
  public void configureMiniAccumulo(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    // Enable native maps by default
    cfg.setNativeLibPaths(NativeMapIT.nativeMapLocation().getAbsolutePath());
    cfg.setProperty(Property.TSERV_NATIVEMAP_ENABLED, Boolean.TRUE.toString());
    Map<String, String> siteConfig = cfg.getSiteConfig();
    if (TRUE.equals(siteConfig.get(Property.INSTANCE_RPC_SSL_ENABLED.getKey()))) {
      throw new RuntimeException("Cannot use both SSL and SASL/Kerberos");
    }

    if (TRUE.equals(siteConfig.get(Property.INSTANCE_RPC_SASL_ENABLED.getKey()))) {
      // already enabled
      return;
    }

    if (null == kdc) {
      throw new IllegalStateException("MiniClusterKdc was null");
    }

    LOGGER.info("Enabling Kerberos/SASL for minicluster");

    // Turn on SASL and set the keytab/principal information
    cfg.setProperty(Property.INSTANCE_RPC_SASL_ENABLED, "true");
    ClusterUser serverUser = kdc.getAccumuloServerUser();
    cfg.setProperty(Property.GENERAL_KERBEROS_KEYTAB, serverUser.getKeytab().getAbsolutePath());
    cfg.setProperty(Property.GENERAL_KERBEROS_PRINCIPAL, serverUser.getPrincipal());
    cfg.setProperty(
        Property.INSTANCE_SECURITY_AUTHENTICATOR,
        KerberosAuthenticator.class.getName());
    cfg.setProperty(Property.INSTANCE_SECURITY_AUTHORIZOR, KerberosAuthorizor.class.getName());
    cfg.setProperty(
        Property.INSTANCE_SECURITY_PERMISSION_HANDLER,
        KerberosPermissionHandler.class.getName());
    // Piggy-back on the "system user" credential, but use it as a normal KerberosToken, not the
    // SystemToken.
    cfg.setProperty(Property.TRACE_USER, serverUser.getPrincipal());
    cfg.setProperty(Property.TRACE_TOKEN_TYPE, KerberosToken.CLASS_NAME);
    // Pass down some KRB5 debug properties
    Map<String, String> systemProperties = cfg.getSystemProperties();
    systemProperties.put(JAVA_SECURITY_KRB5_CONF, System.getProperty(JAVA_SECURITY_KRB5_CONF, ""));
    systemProperties.put(
        SUN_SECURITY_KRB5_DEBUG,
        System.getProperty(SUN_SECURITY_KRB5_DEBUG, "false"));
    cfg.setSystemProperties(systemProperties);
    cfg.setRootUserName(kdc.getRootUser().getPrincipal());
    // Make sure UserGroupInformation will do the correct login
    coreSite.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
  }

  public ClusterUser getRootUser() {
    return rootUser;
  }
}
