/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.utils;

import java.lang.reflect.InvocationTargetException;

public final class InstantiationUtils {
  private InstantiationUtils() {}

  /**
   * Calls the no-arg constructor with the exception contract of the deprecated
   * {@link Class#newInstance()}: a missing no-arg constructor is an {@link InstantiationException}
   * caused by the {@link NoSuchMethodException}, an inaccessible one is an
   * {@link IllegalAccessException}, and whatever the constructor throws, checked or not, is
   * rethrown as is rather than wrapped in an {@link InvocationTargetException}.
   *
   * <p> Access is checked from this class rather than the caller, so in practice the class and its
   * no-arg constructor must be public.
   */
  public static <T> T newInstance(final Class<T> clazz)
      throws InstantiationException, IllegalAccessException {
    try {
      return clazz.getDeclaredConstructor().newInstance();
    } catch (final NoSuchMethodException e) {
      throw (InstantiationException) new InstantiationException(clazz.getName()).initCause(e);
    } catch (final InvocationTargetException e) {
      throw sneakyThrow(e.getCause());
    }
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> RuntimeException sneakyThrow(final Throwable t) throws E {
    throw (E) t;
  }
}
