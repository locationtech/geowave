/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * A result set that wraps a single result.
 */
public class SingletonResultSet implements ResultSet {

  private Result next;

  private final List<String> columnNames;
  private final List<Class<?>> columnTypes;

  /**
   * @param columnNames the display name of each column
   * @param columnTypes the type of each column
   * @param values the values of each column
   */
  public SingletonResultSet(
      final List<String> columnNames,
      final List<Class<?>> columnTypes,
      final List<Object> values) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    next = new Result(values);
  }

  @Override
  public void close() {}

  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public Result next() {
    if (next != null) {
      Result retVal = next;
      next = null;
      return retVal;
    }
    throw new NoSuchElementException();
  }

  @Override
  public int columnCount() {
    return columnNames.size();
  }

  @Override
  public String columnName(final int index) {
    return columnNames.get(index);
  }

  @Override
  public int columnIndex(final String columnName) {
    return columnNames.indexOf(columnName);
  }

  @Override
  public Class<?> columnType(int index) {
    return columnTypes.get(index);
  }

}
