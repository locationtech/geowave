/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

public class GeoWaveConfiguratorBaseTest {
  private enum Key {
    INSTANCE
  }

  public static class DefaultConstructor implements Runnable {
    @Override
    public void run() {}
  }

  public static class FailingConstructor implements Runnable {
    public FailingConstructor() {
      throw new IllegalStateException("constructor failed");
    }

    @Override
    public void run() {}
  }

  public static class NoDefaultConstructor implements Runnable {
    public NoDefaultConstructor(final String unused) {}

    @Override
    public void run() {}
  }

  private static Runnable getInstance(final Class<? extends Runnable> defaultClass)
      throws Exception {
    return GeoWaveConfiguratorBase.getInstance(
        GeoWaveConfiguratorBaseTest.class,
        Key.INSTANCE,
        Job.getInstance(new Configuration(false)),
        Runnable.class,
        defaultClass);
  }

  @Test
  public void testGetInstance() throws Exception {
    assertTrue(getInstance(DefaultConstructor.class) instanceof DefaultConstructor);
  }

  @Test
  public void testConstructorExceptionIsNotWrapped() {
    final IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> getInstance(FailingConstructor.class));
    assertEquals("constructor failed", e.getMessage());
  }

  @Test
  public void testMissingNoArgConstructorIsInstantiationException() {
    final InstantiationException e =
        assertThrows(InstantiationException.class, () -> getInstance(NoDefaultConstructor.class));
    assertTrue(e.getCause() instanceof NoSuchMethodException);
  }
}
