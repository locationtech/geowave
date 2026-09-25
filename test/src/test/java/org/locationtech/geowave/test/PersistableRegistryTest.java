/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.SPIServiceRegistry;
import org.locationtech.geowave.core.index.persist.InternalPersistableRegistry;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistableFactory;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi.PersistableIdAndConstructor;
import org.reflections.Reflections;

public class PersistableRegistryTest {

  @Test
  public void testPersistablesRegistry() {
    final Reflections reflections = new Reflections("org.locationtech.geowave");
    final Set<Class<? extends Persistable>> actual = reflections.getSubTypesOf(Persistable.class);
    final Set<Class<Persistable>> registered =
        PersistableFactory.getInstance().getClassIdMapping().keySet();
    registered.forEach(c -> actual.remove(c));
    Assert.assertFalse(
        Arrays.toString(
            actual.stream().filter(
                c -> !c.isInterface() && !Modifier.isAbstract(c.getModifiers())).toArray(
                    Class[]::new))
            + " are concrete class implementing Persistable but are not registered",
        actual.stream().anyMatch(c -> !c.isInterface() && !Modifier.isAbstract(c.getModifiers())));
  }

  /**
   * PersistableFactory keeps the first of two registrations that collide and only logs the other,
   * which testPersistablesRegistry would report as the second class not being registered at all.
   */
  @Test
  public void testPersistableIdsAreUnique() {
    final Map<Short, String> byId = new HashMap<>();
    final Map<Class<?>, String> byClass = new HashMap<>();
    final List<String> duplicates = new ArrayList<>();
    final Iterator<PersistableRegistrySpi> registries =
        SPIServiceRegistry.load(PersistableRegistrySpi.class);
    while (registries.hasNext()) {
      final PersistableRegistrySpi registry = registries.next();
      for (final PersistableIdAndConstructor p : registry.getSupportedPersistables()) {
        // as PersistableFactory does, third-party registries get the negative ID space
        final short id =
            registry instanceof InternalPersistableRegistry ? p.getPersistableId()
                : (short) -Math.abs(p.getPersistableId());
        final Class<?> persistableClass = p.getPersistableConstructor().get().getClass();
        final String registration =
            persistableClass.getName() + " with ID " + id + " by " + registry.getClass().getName();
        final String sameId = byId.putIfAbsent(id, registration);
        if (sameId != null) {
          duplicates.add(sameId + " and " + registration);
        }
        final String sameClass = byClass.putIfAbsent(persistableClass, registration);
        if ((sameClass != null) && (sameId == null)) {
          duplicates.add(sameClass + " and " + registration);
        }
      }
    }
    Assert.assertTrue(
        "Persistables registered more than once: " + String.join("; ", duplicates),
        duplicates.isEmpty());
  }
}
