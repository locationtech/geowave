/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.gwql.function;

import java.util.function.Supplier;

/**
 * Class for adding functions to the GeoWave query language.
 */
public interface QLFunctionRegistrySpi {

  /**
   * @return the functions to add
   */
  public QLFunctionNameAndConstructor[] getSupportedPersistables();

  /**
   * Associates a {@class QLFunction} with a function name.
   */
  public static class QLFunctionNameAndConstructor {
    private final String functionName;
    private final Supplier<QLFunction> functionConstructor;

    /**
     * @param functionName the name of the function
     * @param functionConstructor the function constructor
     */
    public QLFunctionNameAndConstructor(
        final String functionName,
        final Supplier<QLFunction> functionConstructor) {
      this.functionName = functionName;
      this.functionConstructor = functionConstructor;
    }

    /**
     * @return the name of the function
     */
    public String getFunctionName() {
      return functionName;
    }

    /**
     * @return the function constructor
     */
    public Supplier<QLFunction> getFunctionConstructor() {
      return functionConstructor;
    }
  }
}
