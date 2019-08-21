/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.dimension;

import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.util.GenericTypeResolver;

/**
 * This interface provides in addition to the index dimension definition, a way to read and write a
 * field and get a field ID
 *
 * @param <T>
 */
public interface NumericDimensionField<T extends CommonIndexValue> extends
    NumericDimensionDefinition {
  /**
   * Decode a numeric value or range from the raw field value
   *
   * @param dataElement the raw field value
   * @return a numeric value or range
   */
  NumericData getNumericData(T dataElement);

  /**
   * Returns an identifier that is unique for a given data type (field IDs should be distinct per
   * row)
   *
   * @return the field name
   */
  String getFieldName();

  /**
   * Get a writer that can handle serializing values for this field
   *
   * @return the field writer for this field
   */
  FieldWriter<?, T> getWriter();

  /**
   * Get a reader that can handle deserializing binary data into values for this field
   *
   * @return the field reader for this field
   */
  FieldReader<T> getReader();

  /**
   * Get the basic index definition for this field
   *
   * @return the base index definition for this dimension
   */
  NumericDimensionDefinition getBaseDefinition();

  /**
   * Determines if the given field type is compatible with this field.
   *
   * @param clazz the field type to check
   * @return true if the given field type is assignable
   */
  default boolean isCompatibleWith(final Class<? extends CommonIndexValue> clazz) {
    return GenericTypeResolver.resolveTypeArgument(
        getClass(),
        NumericDimensionField.class).isAssignableFrom(clazz);
  }
}
