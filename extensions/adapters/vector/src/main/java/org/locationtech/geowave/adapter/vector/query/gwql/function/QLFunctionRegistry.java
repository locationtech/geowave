/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.gwql.function;

import java.util.Arrays;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Supplier;
import com.google.common.collect.Maps;

/**
 * Singleton registry for all query language functions. Functions can be added to the language using
 * {@class QLFunctionRegistrySpi}.
 */
public class QLFunctionRegistry {

  private static QLFunctionRegistry INSTANCE = null;

  private Map<String, Supplier<QLFunction>> functions = Maps.newHashMap();

  private QLFunctionRegistry() {
    final ServiceLoader<QLFunctionRegistrySpi> serviceLoader =
        ServiceLoader.load(QLFunctionRegistrySpi.class);
    for (final QLFunctionRegistrySpi functionSet : serviceLoader) {
      Arrays.stream(functionSet.getSupportedPersistables()).forEach(
          f -> functions.put(f.getFunctionName().toUpperCase(), f.getFunctionConstructor()));
    }
  }

  public static QLFunctionRegistry instance() {
    if (INSTANCE == null) {
      INSTANCE = new QLFunctionRegistry();
    }
    return INSTANCE;
  }

  /**
   * Retrieves the function with the given name.
   * 
   * @param functionName the function name
   * @return the function that matches the given name, or {@code null} if it could not be found
   */
  public QLFunction getFunction(final String functionName) {
    Supplier<QLFunction> function = functions.get(functionName.toUpperCase());
    if (function == null) {
      return null;
    }
    return function.get();
  }

}
