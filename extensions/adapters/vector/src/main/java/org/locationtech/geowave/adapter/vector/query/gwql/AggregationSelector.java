package org.locationtech.geowave.adapter.vector.query.gwql;

/**
 * Selector that applies an aggregation function to the query.
 */
public class AggregationSelector extends Selector {
  private final String functionName;
  private final String[] functionArgs;
  private final String name;

  /**
   * @param functionName the name of the function
   * @param functionArgs the function arguments
   */
  public AggregationSelector(final String functionName, final String[] functionArgs) {
    this(functionName, functionArgs, null);
  }

  /**
   * @param functionName the name of the function
   * @param functionArgs the funciton arguments
   * @param alias the column alias of this selector
   */
  public AggregationSelector(
      final String functionName,
      final String[] functionArgs,
      final String alias) {
    super(SelectorType.AGGREGATION, alias);
    this.functionName = functionName;
    this.functionArgs = functionArgs;
    name = functionName.toUpperCase() + "(" + String.join(",", functionArgs) + ")";
  }

  /**
   * @return the function name
   */
  public String functionName() {
    return functionName;
  }

  /**
   * @return the function arguments
   */
  public String[] functionArgs() {
    return functionArgs;
  }

  /**
   * @return the display name of this selector
   */
  @Override
  public String selectorName() {
    return name;
  }



}
