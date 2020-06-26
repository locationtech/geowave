/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.util;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConnectorPool {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorPool.class);
  private static ConnectorPool singletonInstance;

  public static synchronized ConnectorPool getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new ConnectorPool();
    }
    return singletonInstance;
  }

  private final Map<ConnectorConfig, Connector> connectorCache = new HashMap<>();

  public synchronized Connector getConnector(
      final String zookeeperUrl,
      final String instanceName,
      final String userName,
      final String passwordOrKeyTab,
      boolean useSasl) throws AccumuloException, AccumuloSecurityException, IOException {

    final ConnectorConfig config =
        new ConnectorConfig(zookeeperUrl, instanceName, userName, passwordOrKeyTab, useSasl);
    Connector connector = connectorCache.get(config);
    if (connector == null) {

      ClientConfiguration conf =
          // ClientConfiguration.create().withInstance(instanceName).withZkHosts(zookeeperUrl);

          // using deprecated constructor for accumulo 1.7 compatibility
          new ClientConfiguration().withInstance(instanceName).withZkHosts(zookeeperUrl);
      if (useSasl) {
        conf.withSasl(true);
        File file = new java.io.File(passwordOrKeyTab);
        UserGroupInformation.loginUserFromKeytab(userName, file.getAbsolutePath());

        // using deprecated constructor with replaceCurrentUser=false for accumulo 1.7 compatibility
        connector =
            new ZooKeeperInstance(conf).getConnector(
                userName,
                new KerberosToken(userName, file, false));
        // If on a secured cluster, create a thread to periodically renew Kerberos tgt
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

        executor.scheduleAtFixedRate(() -> {
          try {
            UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
          } catch (Exception e) {
            LOGGER.warn("Unable to renew Kerberos TGT", e);
          }
        }, 0, 10, TimeUnit.MINUTES);

      } else {
        connector =
            new ZooKeeperInstance(conf).getConnector(userName, new PasswordToken(passwordOrKeyTab));
      }
      connectorCache.put(config, connector);
    }
    return connector;
  }

  private static class ConnectorConfig {
    private final String zookeeperUrl;
    private final String instanceName;
    private final String userName;
    private final String passwordOrKeyTab;
    private final boolean useSasl;

    public ConnectorConfig(
        final String zookeeperUrl,
        final String instanceName,
        final String userName,
        final String passwordOrKeyTab,
        boolean useSasl) {
      this.zookeeperUrl = zookeeperUrl;
      this.instanceName = instanceName;
      this.userName = userName;
      this.passwordOrKeyTab = passwordOrKeyTab;
      this.useSasl = useSasl;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      ConnectorConfig that = (ConnectorConfig) o;
      return useSasl == that.useSasl
          && zookeeperUrl.equals(that.zookeeperUrl)
          && instanceName.equals(that.instanceName)
          && userName.equals(that.userName)
          && Objects.equals(passwordOrKeyTab, that.passwordOrKeyTab);
    }

    @Override
    public int hashCode() {
      return Objects.hash(zookeeperUrl, instanceName, userName, passwordOrKeyTab, useSasl);
    }
  }
}
