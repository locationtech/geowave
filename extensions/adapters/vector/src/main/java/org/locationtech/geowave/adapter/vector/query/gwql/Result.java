/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
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

  public List<Object> values() {
    return values;
  }

}
