/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter.annotation;

import java.lang.reflect.Field;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.FieldDescriptorBuilder;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;

/**
 * Base implementation for annotated field descriptor builders. This builder is used by the
 * `@GeoWaveField` annotation.
 */
public class BaseAnnotatedFieldDescriptorBuilder implements AnnotatedFieldDescriptorBuilder {
  @Override
  public FieldDescriptor<?> buildFieldDescriptor(Field field) {
    if (field.isAnnotationPresent(GeoWaveField.class)) {
      final GeoWaveField fieldAnnotation = field.getAnnotation(GeoWaveField.class);
      final String fieldName;
      if (fieldAnnotation.name().isEmpty()) {
        fieldName = field.getName();
      } else {
        fieldName = fieldAnnotation.name();
      }
      final String[] indexHints = fieldAnnotation.indexHints();
      final FieldDescriptorBuilder<?, ?, ?> builder =
          new FieldDescriptorBuilder<>(BasicDataTypeAdapter.normalizeClass(field.getType()));
      for (final String hint : indexHints) {
        builder.indexHint(new IndexDimensionHint(hint));
      }
      return builder.fieldName(fieldName).build();
    }
    throw new RuntimeException("Field is missing GeoWaveField annotation.");
  }
}
