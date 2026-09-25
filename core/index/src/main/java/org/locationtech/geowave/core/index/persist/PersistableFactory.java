/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.persist;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;
import org.locationtech.geowave.core.index.SPIServiceRegistry;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi.PersistableIdAndConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistableFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PersistableFactory.class);

  private final Map<Class<Persistable>, Short> classRegistry;

  private final Map<Short, Supplier<Persistable>> constructorRegistry;

  // the class and registry behind each ID, so that a collision can name both sides of it
  private final Map<Short, Registration> registrations;

  private static PersistableFactory singletonInstance = null;

  public static synchronized PersistableFactory getInstance() {
    if (singletonInstance == null) {
      final PersistableFactory internalFactory = new PersistableFactory();
      final Iterator<PersistableRegistrySpi> persistableRegistries =
          SPIServiceRegistry.load(PersistableRegistrySpi.class);
      while (persistableRegistries.hasNext()) {
        final PersistableRegistrySpi persistableRegistry = persistableRegistries.next();
        if (persistableRegistry != null) {
          internalFactory.addRegistry(persistableRegistry);
        }
      }
      singletonInstance = internalFactory;
    }
    return singletonInstance;
  }

  PersistableFactory() {
    classRegistry = new HashMap<>();
    constructorRegistry = new HashMap<>();
    registrations = new HashMap<>();
  }

  protected void addRegistry(final PersistableRegistrySpi registry) {
    final PersistableIdAndConstructor[] persistables = registry.getSupportedPersistables();
    final boolean external = !(registry instanceof InternalPersistableRegistry);
    for (final PersistableIdAndConstructor p : persistables) {
      addPersistableType(
          registry.getClass(),
          external ? (short) (-Math.abs(p.getPersistableId())) : p.getPersistableId(),
          p.getPersistableConstructor());
    }
  }

  protected void addPersistableType(
      final Class<?> registry,
      final short persistableId,
      final Supplier<Persistable> constructor) {
    final Class persistableClass = constructor.get().getClass();
    final Short existingId = classRegistry.get(persistableClass);
    if (existingId != null) {
      LOGGER.error(
          persistableClass.getName()
              + " is registered twice, with persistable ID "
              + existingId
              + " by "
              + registrations.get(existingId).registry.getName()
              + " and with ID "
              + persistableId
              + " by "
              + registry.getName()
              + "; only ID "
              + existingId
              + " is used");
      return;
    }
    final Registration existing = registrations.get(persistableId);
    if (existing != null) {
      LOGGER.error(
          "Persistable ID "
              + persistableId
              + " is registered to both "
              + existing.persistableClass.getName()
              + " by "
              + existing.registry.getName()
              + " and "
              + persistableClass.getName()
              + " by "
              + registry.getName()
              + "; "
              + persistableClass.getName()
              + " is not registered, so it cannot be persisted or read");
      return;
    }
    classRegistry.put(persistableClass, persistableId);
    constructorRegistry.put(persistableId, constructor);
    registrations.put(persistableId, new Registration(persistableClass, registry));
  }

  public Persistable newInstance(final short id) {
    final Supplier<Persistable> constructor = constructorRegistry.get(id);
    if (constructor != null) {
      return constructor.get();
    }
    return null;
  }

  public Map<Class<Persistable>, Short> getClassIdMapping() {
    return classRegistry;
  }

  private static class Registration {
    private final Class<?> persistableClass;
    private final Class<?> registry;

    private Registration(final Class<?> persistableClass, final Class<?> registry) {
      this.persistableClass = persistableClass;
      this.registry = registry;
    }
  }
}
