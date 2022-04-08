/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.adapter.annotation;

import java.lang.reflect.Field;
import org.locationtech.geowave.core.geotime.adapter.TemporalFieldDescriptorBuilder;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.adapter.annotation.AnnotatedFieldDescriptorBuilder;

/**
 * Builds spatial field descriptors for fields annotated with `@GeoWaveSpatialField`.
 */
public class TemporalAnnotatedFieldDescriptorBuilder implements AnnotatedFieldDescriptorBuilder {
  @Override
  public FieldDescriptor<?> buildFieldDescriptor(Field field) {
    if (field.isAnnotationPresent(GeoWaveTemporalField.class)) {
      final GeoWaveTemporalField fieldAnnotation = field.getAnnotation(GeoWaveTemporalField.class);
      final String fieldName;
      if (fieldAnnotation.name().isEmpty()) {
        fieldName = field.getName();
      } else {
        fieldName = fieldAnnotation.name();
      }
      final String[] indexHints = fieldAnnotation.indexHints();
      final TemporalFieldDescriptorBuilder<?> builder =
          new TemporalFieldDescriptorBuilder<>(
              BasicDataTypeAdapter.normalizeClass(field.getType()));
      for (final String hint : indexHints) {
        builder.indexHint(new IndexDimensionHint(hint));
      }
      if (fieldAnnotation.timeIndexHint()) {
        builder.timeIndexHint();
      }
      if (fieldAnnotation.startTimeIndexHint()) {
        builder.startTimeIndexHint();
      }
      if (fieldAnnotation.endTimeIndexHint()) {
        builder.endTimeIndexHint();
      }
      return builder.fieldName(fieldName).build();
    }
    throw new RuntimeException("Field is missing GeoWaveTemporalField annotation.");
  }
}

