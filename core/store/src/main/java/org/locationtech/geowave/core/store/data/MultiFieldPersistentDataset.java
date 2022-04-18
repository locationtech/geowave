/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data;

import java.util.HashMap;
import java.util.Map;

/**
 * This is a basic mapping of field ID to native field type. "Native" in this sense can be to either
 * the data adapter or the common index, depending on whether it is in the common index or is an
 * extended field.
 *
 * @param <T> The most specific generalization for the type for all of the values in this dataset.
 */
public class MultiFieldPersistentDataset<T> implements PersistentDataset<T> {
  private final Map<String, T> fieldNameToValueMap;

  public MultiFieldPersistentDataset() {
    fieldNameToValueMap = new HashMap<>();
  }

  public MultiFieldPersistentDataset(final String fieldName, final T value) {
    this();
    addValue(fieldName, value);
  }

  public MultiFieldPersistentDataset(final Map<String, T> fieldIdToValueMap) {
    this.fieldNameToValueMap = fieldIdToValueMap;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.locationtech.geowave.core.store.data.PersistentDataSet#addValue(java.lang.String, T)
   */
  @Override
  public void addValue(final String fieldName, final T value) {
    fieldNameToValueMap.put(fieldName, value);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.locationtech.geowave.core.store.data.PersistentDataSet#addValues(java.util.Map)
   */
  @Override
  public void addValues(final Map<String, T> values) {
    fieldNameToValueMap.putAll(values);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.locationtech.geowave.core.store.data.PersistentDataSet#getValue(java.lang.String)
   */
  @Override
  public T getValue(final String fieldName) {
    return fieldNameToValueMap.get(fieldName);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.locationtech.geowave.core.store.data.PersistentDataSet#getValues()
   */
  @Override
  public Map<String, T> getValues() {
    return fieldNameToValueMap;
  }
}
