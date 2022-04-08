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
import java.util.Arrays;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistableFactory;
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
}
