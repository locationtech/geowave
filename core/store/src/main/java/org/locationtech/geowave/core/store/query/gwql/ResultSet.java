/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql;

import org.locationtech.geowave.core.store.CloseableIterator;

/**
 * Interface for a set of results from a GeoWave query.
 */
public interface ResultSet extends CloseableIterator<Result> {

  /**
   * @return the number of columns that each result contains
   */
  public int columnCount();

  /**
   * @param index the index of the column
   * @return the display name of the column at the given index
   */
  public String columnName(final int index);

  /**
   * @param columnName the name of the column to find
   * @return the index of the column with the given display name
   */
  public int columnIndex(final String columnName);

  /**
   * @param index the index of the column
   * @return the Class of the objects that can be found in the given column
   */
  public Class<?> columnType(final int index);
}
