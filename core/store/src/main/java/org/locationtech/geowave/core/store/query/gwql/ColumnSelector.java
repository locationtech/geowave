/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql;

/**
 * Selector that pulls a value from a single column of the results set.
 */
public class ColumnSelector extends Selector {

  private final String columnName;

  /**
   * @param columnName the column to select
   */
  public ColumnSelector(final String columnName) {
    this(columnName, null);
  }

  /**
   * @param columnName the column to select
   * @param alias the alias of the column
   */
  public ColumnSelector(final String columnName, final String alias) {
    super(SelectorType.SIMPLE, alias);
    this.columnName = columnName;
  }

  /**
   * @return the selected column name
   */
  public String columnName() {
    return columnName;
  }

  /**
   * @return the display name of this selector
   */
  @Override
  public String selectorName() {
    return columnName;
  }

}
