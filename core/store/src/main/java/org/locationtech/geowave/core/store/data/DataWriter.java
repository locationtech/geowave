/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data;

import org.locationtech.geowave.core.store.data.field.FieldWriter;

/**
 * This interface is used to write data for a row in a GeoWave data store.
 *
 * @param <RowType> The binding class of this row
 * @param <FieldType> The binding class of this field
 */
public interface DataWriter<RowType, FieldType> {
  /**
   * Get a writer for an individual field given the ID.
   *
   * @param fieldName the unique field ID
   * @return the writer for the given field
   */
  public FieldWriter<RowType, FieldType> getWriter(String fieldName);
}
