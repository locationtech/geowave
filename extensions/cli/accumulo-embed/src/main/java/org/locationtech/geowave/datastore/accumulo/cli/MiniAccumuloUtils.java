/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.cli;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Because the impl package changed between Accumulo 1.x and 2.x we are using this to access methods
 * in impl without requiring the impl package name
 *
 */
public class MiniAccumuloUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(MiniAccumuloUtils.class);

  public static void setClasspathItems(
      final MiniAccumuloConfig config,
      final String... classpathItems) {
    try {
      final Field impl = MiniAccumuloConfig.class.getDeclaredField("impl");
      impl.setAccessible(true);
      impl.getType().getMethod("setClasspathItems", String[].class).invoke(
          impl.get(config),
          new Object[] {classpathItems});
    } catch (final Exception e) {
      LOGGER.warn("Unable to setClasspathItems", e);
    }
  }

  public static void setRootUserName(final MiniAccumuloConfig config, final String rootUserName) {
    try {
      final Field impl = MiniAccumuloConfig.class.getDeclaredField("impl");
      impl.setAccessible(true);
      impl.getType().getMethod("setRootUserName", String.class).invoke(
          impl.get(config),
          rootUserName);
    } catch (final Exception e) {
      LOGGER.warn("Unable to setRootUserName", e);
    }
  }

  public static Object getClientProperty(final String name) {
    try {
      return MiniAccumuloUtils.class.getClassLoader().loadClass(
          "org.apache.accumulo.core.conf.ClientProperty").getDeclaredMethod(
              "valueOf",
              String.class).invoke(null, name);
    } catch (final Exception e) {
      LOGGER.warn("Unable to getClientProperty", e);
    }
    return null;
  }

  public static void setClientProperty(
      final MiniAccumuloConfig config,
      final Object property,
      final String value) {
    try {
      final Field impl = MiniAccumuloConfig.class.getDeclaredField("impl");
      impl.setAccessible(true);
      impl.getType().getMethod(
          "setClientProperty",
          MiniAccumuloUtils.class.getClassLoader().loadClass(
              "org.apache.accumulo.core.conf.ClientProperty"),
          String.class).invoke(impl.get(config), property, value);
    } catch (final Exception e) {
      LOGGER.warn("Unable to setClientProperty", e);
    }
  }

  public static void setProperty(
      final MiniAccumuloConfig config,
      final Property property,
      final String value) {
    try {
      final Field impl = MiniAccumuloConfig.class.getDeclaredField("impl");
      impl.setAccessible(true);
      impl.getType().getMethod("setProperty", Property.class, String.class).invoke(
          impl.get(config),
          property,
          value);
    } catch (final Exception e) {
      LOGGER.warn("Unable to setProperty", e);
    }
  }

  public static Map<String, String> getSiteConfig(final MiniAccumuloConfig config) {
    try {
      final Field impl = MiniAccumuloConfig.class.getDeclaredField("impl");
      impl.setAccessible(true);
      return (Map<String, String>) impl.getType().getMethod("getSiteConfig").invoke(
          impl.get(config));
    } catch (final Exception e) {
      LOGGER.warn("Unable to getSiteConfig", e);
    }
    return null;
  }

  public static Map<String, String> getSystemProperties(final MiniAccumuloConfig config) {
    try {
      final Field impl = MiniAccumuloConfig.class.getDeclaredField("impl");
      impl.setAccessible(true);
      return (Map<String, String>) impl.getType().getMethod("getSystemProperties").invoke(
          impl.get(config));
    } catch (final Exception e) {
      LOGGER.warn("Unable to getSystemProperties", e);
    }
    return null;
  }

  public static void setSystemProperties(
      final MiniAccumuloConfig config,
      final Map<String, String> systemProperties) {
    try {
      final Field impl = MiniAccumuloConfig.class.getDeclaredField("impl");
      impl.setAccessible(true);
      impl.getType().getMethod("setSystemProperties", Map.class).invoke(
          impl.get(config),
          systemProperties);
    } catch (final Exception e) {
      LOGGER.warn("Unable to setSystemProperties", e);
    }
  }

  public static File getConfDir(final MiniAccumuloConfig config) throws IOException {
    try {
      final Field impl = MiniAccumuloConfig.class.getDeclaredField("impl");
      impl.setAccessible(true);
      return (File) impl.getType().getMethod("getConfDir").invoke(impl.get(config));
    } catch (final Exception e) {
      LOGGER.warn("Unable to getConfDir", e);
    }
    return null;
  }

  public static File getLogDir(final MiniAccumuloConfig config) throws IOException {
    try {
      final Field impl = MiniAccumuloConfig.class.getDeclaredField("impl");
      impl.setAccessible(true);
      return (File) impl.getType().getMethod("getLogDir").invoke(impl.get(config));
    } catch (final Exception e) {
      LOGGER.warn("Unable to getLogDir", e);
    }
    return null;
  }

  public static String getZooKeepers(final MiniAccumuloConfig config) throws IOException {
    try {
      final Field impl = MiniAccumuloConfig.class.getDeclaredField("impl");
      impl.setAccessible(true);
      return (String) impl.getType().getMethod("getZooKeepers").invoke(impl.get(config));
    } catch (final Exception e) {
      LOGGER.warn("Unable to getZooKeepers", e);
    }
    return null;
  }

  public static Process exec(
      final MiniAccumuloCluster cluster,
      final Class<?> clazz,
      final String... args) throws IOException {
    return exec(cluster, clazz, null, args);
  }

  public static Process exec(
      final MiniAccumuloCluster cluster,
      final Class<?> clazz,
      final List<String> jvmArgs,
      final String... args) throws IOException {
    try {
      final Field impl = MiniAccumuloCluster.class.getDeclaredField("impl");
      impl.setAccessible(true);
      final Object obj =
          impl.getType().getMethod("exec", Class.class, List.class, String[].class).invoke(
              impl.get(cluster),
              clazz,
              jvmArgs,
              args);
      if (obj instanceof Process) {
        return (Process) obj;
      } else {
        return (Process) obj.getClass().getMethod("getProcess").invoke(obj);
      }
    } catch (final Exception e) {
      LOGGER.warn("Unable start process for " + clazz, e);
    }
    return null;
  }
}
