/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import org.junit.Test;

public class InstantiationUtilsTest {
  private static final IOException CHECKED = new IOException("checked");
  private static final IllegalStateException UNCHECKED = new IllegalStateException("unchecked");
  private static final NoClassDefFoundError ERROR = new NoClassDefFoundError("some/Missing");

  public static class NoArg {
  }

  public static class ThrowsChecked {
    public ThrowsChecked() throws IOException {
      throw CHECKED;
    }
  }

  public static class ThrowsUnchecked {
    public ThrowsUnchecked() {
      throw UNCHECKED;
    }
  }

  public static class ThrowsError {
    public ThrowsError() {
      throw ERROR;
    }
  }

  public static class NoNoArgConstructor {
    public NoNoArgConstructor(final String unused) {}
  }

  public static class PrivateConstructor {
    private PrivateConstructor() {}
  }

  @Test
  public void testNewInstance() throws Exception {
    assertTrue(InstantiationUtils.newInstance(NoArg.class) instanceof NoArg);
  }

  @Test
  public void testCheckedConstructorExceptionIsRethrownUnchanged() {
    assertSame(
        CHECKED,
        assertThrows(IOException.class, () -> InstantiationUtils.newInstance(ThrowsChecked.class)));
  }

  @Test
  public void testUncheckedConstructorExceptionIsRethrownUnchanged() {
    assertSame(
        UNCHECKED,
        assertThrows(
            IllegalStateException.class,
            () -> InstantiationUtils.newInstance(ThrowsUnchecked.class)));
  }

  @Test
  public void testConstructorErrorIsRethrownUnchanged() {
    assertSame(
        ERROR,
        assertThrows(
            NoClassDefFoundError.class,
            () -> InstantiationUtils.newInstance(ThrowsError.class)));
  }

  @Test
  public void testMissingNoArgConstructorIsInstantiationException() {
    final InstantiationException e =
        assertThrows(
            InstantiationException.class,
            () -> InstantiationUtils.newInstance(NoNoArgConstructor.class));
    assertEquals(NoNoArgConstructor.class.getName(), e.getMessage());
    assertTrue(e.getCause() instanceof NoSuchMethodException);
  }

  @Test
  public void testPrivateConstructorIsIllegalAccessException() {
    assertThrows(
        IllegalAccessException.class,
        () -> InstantiationUtils.newInstance(PrivateConstructor.class));
  }
}
