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
