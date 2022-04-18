/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.util;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectorPool {
  public static interface ConnectorCloseListener {
    void notifyConnectorClosed();
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorPool.class);
  private static ConnectorPool singletonInstance;

  public static synchronized ConnectorPool getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new ConnectorPool();
    }
    return singletonInstance;
  }

  private final Map<ConnectorConfig, Pair<Connector, Set<ConnectorCloseListener>>> connectorCache =
      new HashMap<>();

  public synchronized Connector getConnector(
      final String zookeeperUrl,
      final String instanceName,
      final String userName,
      final String passwordOrKeyTab,
      final boolean useSasl,
      // the close listener is to ensure all references of this connection are notified
      @Nullable final ConnectorCloseListener closeListener)
      throws AccumuloException, AccumuloSecurityException, IOException {

    final ConnectorConfig config =
        new ConnectorConfig(zookeeperUrl, instanceName, userName, passwordOrKeyTab, useSasl);
    final Connector connector;
    final Pair<Connector, Set<ConnectorCloseListener>> value = connectorCache.get(config);
    if (value == null) {
      final ClientConfiguration conf =
          ClientConfiguration.create().withInstance(instanceName).withZkHosts(zookeeperUrl);

      if (useSasl) {
        conf.withSasl(true);
        final File file = new java.io.File(passwordOrKeyTab);
        UserGroupInformation.loginUserFromKeytab(userName, file.getAbsolutePath());

        // using deprecated constructor with replaceCurrentUser=false for accumulo 1.7 compatibility
        connector =
            new ZooKeeperInstance(conf).getConnector(userName, new KerberosToken(userName, file));
        // If on a secured cluster, create a thread to periodically renew Kerberos tgt
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

        executor.scheduleAtFixedRate(() -> {
          try {
            UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
          } catch (final Exception e) {
            LOGGER.warn("Unable to renew Kerberos TGT", e);
          }
        }, 0, 2, TimeUnit.MINUTES);

      } else {
        connector =
            new ZooKeeperInstance(conf).getConnector(userName, new PasswordToken(passwordOrKeyTab));
      }
      final Set<ConnectorCloseListener> closeListeners = new HashSet<>();
      if (closeListener != null) {
        closeListeners.add(closeListener);
      }
      connectorCache.put(config, Pair.of(connector, closeListeners));
    } else {
      connector = value.getLeft();
      if (closeListener != null) {
        value.getRight().add(closeListener);
      }
    }
    return connector;
  }

  public synchronized void invalidate(final Connector connector) {
    // first find the key that matches this connector, then remove it
    ConnectorConfig key = null;
    for (final Entry<ConnectorConfig, Pair<Connector, Set<ConnectorCloseListener>>> entry : connectorCache.entrySet()) {
      if (connector.equals(entry.getValue().getKey())) {
        key = entry.getKey();
        entry.getValue().getValue().forEach(ConnectorCloseListener::notifyConnectorClosed);
        break;
      }
    }
    if (key != null) {
      connectorCache.remove(key);
    }
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
        final boolean useSasl) {
      this.zookeeperUrl = zookeeperUrl;
      this.instanceName = instanceName;
      this.userName = userName;
      this.passwordOrKeyTab = passwordOrKeyTab;
      this.useSasl = useSasl;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if ((o == null) || (getClass() != o.getClass())) {
        return false;
      }
      final ConnectorConfig that = (ConnectorConfig) o;
      return (useSasl == that.useSasl)
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
