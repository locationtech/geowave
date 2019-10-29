package org.locationtech.geowave.adapter.vector.query.gwql;

import java.util.List;

/**
 * A single immutable query result.
 */
public class Result {
  private final List<Object> values;

  /**
   * @param values the column values of this result
   */
  public Result(List<Object> values) {
    this.values = values;
  }

  /**
   * @param index the column index to get
   * @return the value of the column at the given index for this result
   */
  public Object columnValue(final int index) {
    return values.get(index);
  }

}
