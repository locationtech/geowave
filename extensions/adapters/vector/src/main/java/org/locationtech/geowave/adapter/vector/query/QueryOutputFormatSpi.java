package org.locationtech.geowave.adapter.vector.query;

import org.locationtech.geowave.adapter.vector.query.gwql.ResultSet;

/**
 * Output ResultSets from geowave queries.
 */
public abstract class QueryOutputFormatSpi {
  private final String name;

  protected QueryOutputFormatSpi(final String name) {
    this.name = name;
  }

  /**
   * @return The name of the output format.
   */
  public final String name() {
    return name;
  }

  /**
   * Output the results.
   * 
   * @param results the results of a geowave query
   */
  public abstract void output(final ResultSet results);
}
