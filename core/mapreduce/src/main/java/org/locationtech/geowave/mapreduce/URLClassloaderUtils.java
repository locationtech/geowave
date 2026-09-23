/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce;

import java.net.MalformedURLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.locationtech.geowave.core.index.SPIServiceRegistry;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.util.ClasspathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URLClassloaderUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(URLClassloaderUtils.class);
  private static final Object MUTEX = new Object();
  private static Set<ClassLoader> initializedClassLoaders = new HashSet<>();

  public static void initClassLoader() throws MalformedURLException {
    synchronized (MUTEX) {
      final ClassLoader myCl = URLClassloaderUtils.class.getClassLoader();
      if (initializedClassLoaders.contains(myCl)) {
        return;
      }
      final ClassLoader classLoader = ClasspathUtils.transformClassLoader(myCl);
      if (classLoader != null) {
        SPIServiceRegistry.registerClassLoader(classLoader);
      }
      initializedClassLoaders.add(myCl);
    }
  }

  public static byte[] toBinary(final Persistable persistable) {
    try {
      initClassLoader();
    } catch (final MalformedURLException e) {
      LOGGER.warn("Unable to initialize classloader in toBinary", e);
    }
    return PersistenceUtils.toBinary(persistable);
  }

  public static Persistable fromBinary(final byte[] bytes) {
    try {
      initClassLoader();
    } catch (final MalformedURLException e) {
      LOGGER.warn("Unable to initialize classloader in fromBinary", e);
    }
    return PersistenceUtils.fromBinary(bytes);
  }

  public static byte[] toBinary(final Collection<? extends Persistable> persistables) {
    try {
      initClassLoader();
    } catch (final MalformedURLException e) {
      LOGGER.warn("Unable to initialize classloader in toBinary (list)", e);
    }
    return PersistenceUtils.toBinary(persistables);
  }

  public static byte[] toClassId(final Persistable persistable) {
    try {
      initClassLoader();
    } catch (final MalformedURLException e) {
      LOGGER.warn("Unable to initialize classloader in toClassId", e);
    }
    return PersistenceUtils.toClassId(persistable);
  }

  public static Persistable fromClassId(final byte[] bytes) {
    try {
      initClassLoader();
    } catch (final MalformedURLException e) {
      LOGGER.warn("Unable to initialize classloader in fromClassId", e);
    }
    return PersistenceUtils.fromClassId(bytes);
  }

  public static byte[] toClassId(final String className) {
    try {
      initClassLoader();
    } catch (final MalformedURLException e) {
      LOGGER.warn("Unable to initialize classloader in toClassId(className)", e);
    }
    return PersistenceUtils.toClassId(className);
  }

  public static List<Persistable> fromBinaryAsList(final byte[] bytes) {
    try {
      initClassLoader();
    } catch (final MalformedURLException e) {
      LOGGER.warn("Unable to initialize classloader in fromBinaryAsList", e);
    }
    return PersistenceUtils.fromBinaryAsList(bytes);
  }
}
