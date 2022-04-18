/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import java.util.Map;

/**
 * Interface for building data type rows from a set of field values.
 *
 * @param <T> the data type
 */
public interface RowBuilder<T> {
  /**
   * Set a field name/value pair
   *
   * @param fieldValue the field ID/value pair
   */
  void setField(String fieldName, Object fieldValue);

  /**
   * Sets a set of fields on the row builder
   * 
   * @param values the values to set
   */
  void setFields(Map<String, Object> values);

  /**
   * Create a row with the previously set fields
   *
   * @param dataId the unique data ID for the row
   * @return the row
   */
  T buildRow(byte[] dataId);
}
