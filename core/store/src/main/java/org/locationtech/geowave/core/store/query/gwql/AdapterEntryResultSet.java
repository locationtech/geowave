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
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import com.google.common.collect.Lists;

/**
 * A result set that wraps adapter entries using a given set of column selectors.
 */
public class AdapterEntryResultSet<T> implements ResultSet {

  private final List<Selector> selectors;
  private final DataTypeAdapter<T> adapter;
  private final CloseableIterator<T> entries;

  /**
   * @param selectors the columns to select from the entries
   * @param adapter the data type adapter
   * @param entries the query results
   */
  public AdapterEntryResultSet(
      final List<Selector> selectors,
      final DataTypeAdapter<T> adapter,
      final CloseableIterator<T> entries) {
    this.selectors = selectors;
    this.adapter = adapter;
    this.entries = entries;
  }

  @Override
  public void close() {
    entries.close();
  }

  @Override
  public boolean hasNext() {
    return entries.hasNext();
  }

  @Override
  public Result next() {
    T entry = entries.next();
    List<Object> values = Lists.newArrayListWithCapacity(selectors.size());
    for (Selector column : selectors) {
      if (column instanceof ColumnSelector) {
        values.add(adapter.getFieldValue(entry, ((ColumnSelector) column).columnName()));
      }
    }

    return new Result(values);
  }

  @Override
  public int columnCount() {
    return selectors.size();
  }

  @Override
  public String columnName(final int index) {
    return selectors.get(index).name();
  }

  @Override
  public int columnIndex(final String columnName) {
    for (int i = 0; i < selectors.size(); i++) {
      if (selectors.get(i).name().equals(columnName)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public Class<?> columnType(final int index) {
    ColumnSelector column = (ColumnSelector) selectors.get(index);
    return adapter.getFieldDescriptor(column.columnName()).bindingClass();
  }

  /**
   * @return the adapter
   */
  public DataTypeAdapter<T> getAdapter() {
    return adapter;
  }

}
