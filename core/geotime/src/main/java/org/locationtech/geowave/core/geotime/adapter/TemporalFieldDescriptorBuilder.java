/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.adapter;

import org.locationtech.geowave.core.store.adapter.FieldDescriptorBuilder;
import static org.locationtech.geowave.core.geotime.store.dimension.TimeField.TIME_DIMENSION_HINT;
import static org.locationtech.geowave.core.geotime.store.dimension.TimeField.START_TIME_DIMENSION_HINT;
import static org.locationtech.geowave.core.geotime.store.dimension.TimeField.END_TIME_DIMENSION_HINT;

/**
 * A field descriptor builder for adapter fields that contain time information.
 *
 * @param <T> the adapter field type
 */
public class TemporalFieldDescriptorBuilder<T> extends
    FieldDescriptorBuilder<T, TemporalFieldDescriptor<T>, TemporalFieldDescriptorBuilder<T>> {
  public TemporalFieldDescriptorBuilder(final Class<T> bindingClass) {
    super(bindingClass);
  }

  /**
   * Hint that the field is a time instant and should be used for temporal indexing.
   * 
   * @return the temporal field descriptor builder
   */
  public TemporalFieldDescriptorBuilder<T> timeIndexHint() {
    return this.indexHint(TIME_DIMENSION_HINT);
  }

  /**
   * Hint that the field is the start of a time range and should be used for temporal indexing.
   * There should be a corresponding end time index hint specified in the schema.
   * 
   * @return the temporal field descriptor builder
   */
  public TemporalFieldDescriptorBuilder<T> startTimeIndexHint() {
    return this.indexHint(START_TIME_DIMENSION_HINT);
  }

  /**
   * Hint that the field is the end of a time range and should be used for temporal indexing. There
   * should be a corresponding start time index hint specified in the schema.
   * 
   * @return the temporal field descriptor builder
   */
  public TemporalFieldDescriptorBuilder<T> endTimeIndexHint() {
    return this.indexHint(END_TIME_DIMENSION_HINT);
  }

  @Override
  public TemporalFieldDescriptor<T> build() {
    return new TemporalFieldDescriptor<>(bindingClass, fieldName, indexHints);
  }
}
