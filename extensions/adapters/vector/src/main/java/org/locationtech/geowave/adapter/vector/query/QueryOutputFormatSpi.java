/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
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
