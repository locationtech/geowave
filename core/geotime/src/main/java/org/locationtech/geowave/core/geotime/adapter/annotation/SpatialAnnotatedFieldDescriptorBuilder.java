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
import org.geotools.referencing.CRS;
import org.locationtech.geowave.core.geotime.adapter.SpatialFieldDescriptorBuilder;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.adapter.annotation.AnnotatedFieldDescriptorBuilder;
import org.opengis.referencing.FactoryException;

/**
 * Builds spatial field descriptors for fields annotated with `@GeoWaveSpatialField`.
 */
public class SpatialAnnotatedFieldDescriptorBuilder implements AnnotatedFieldDescriptorBuilder {
  @Override
  public FieldDescriptor<?> buildFieldDescriptor(Field field) {
    if (field.isAnnotationPresent(GeoWaveSpatialField.class)) {
      final GeoWaveSpatialField fieldAnnotation = field.getAnnotation(GeoWaveSpatialField.class);
      final String fieldName;
      if (fieldAnnotation.name().isEmpty()) {
        fieldName = field.getName();
      } else {
        fieldName = fieldAnnotation.name();
      }
      final String[] indexHints = fieldAnnotation.indexHints();
      final SpatialFieldDescriptorBuilder<?> builder =
          new SpatialFieldDescriptorBuilder<>(BasicDataTypeAdapter.normalizeClass(field.getType()));
      for (final String hint : indexHints) {
        builder.indexHint(new IndexDimensionHint(hint));
      }
      if (!fieldAnnotation.crs().isEmpty()) {
        try {
          builder.crs(CRS.decode(fieldAnnotation.crs()));
        } catch (FactoryException e) {
          throw new RuntimeException("Unable to decode CRS: " + fieldAnnotation.crs(), e);
        }
      }
      if (fieldAnnotation.spatialIndexHint()) {
        builder.spatialIndexHint();
      }
      if (fieldAnnotation.latitudeIndexHint()) {
        builder.latitudeIndexHint();
      }
      if (fieldAnnotation.longitudeIndexHint()) {
        builder.longitudeIndexHint();
      }
      return builder.fieldName(fieldName).build();
    }
    throw new RuntimeException("Field is missing GeoWaveSpatialField annotation.");
  }
}
