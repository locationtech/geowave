/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.index;

import java.util.Set;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.simple.SimpleByteIndexStrategy;
import org.locationtech.geowave.core.index.simple.SimpleDoubleIndexStrategy;
import org.locationtech.geowave.core.index.simple.SimpleFloatIndexStrategy;
import org.locationtech.geowave.core.index.simple.SimpleIntegerIndexStrategy;
import org.locationtech.geowave.core.index.simple.SimpleLongIndexStrategy;
import org.locationtech.geowave.core.index.simple.SimpleShortIndexStrategy;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.AttributeIndex;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.dimension.BasicNumericDimensionField;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Sets;

/**
 * Provides attribute indices for numeric fields.
 */
public class NumericAttributeIndexProvider implements AttributeIndexProviderSpi {
  private static Set<Class<?>> SUPPORTED_CLASSES =
      Sets.newHashSet(
          Byte.class,
          Short.class,
          Integer.class,
          Long.class,
          Float.class,
          Double.class);

  @Override
  public boolean supportsDescriptor(final FieldDescriptor<?> fieldDescriptor) {
    return SUPPORTED_CLASSES.contains(fieldDescriptor.bindingClass());
  }

  @Override
  public AttributeIndex buildIndex(
      final String indexName,
      final DataTypeAdapter<?> adapter,
      final FieldDescriptor<?> fieldDescriptor) {
    final Class<?> bindingClass = fieldDescriptor.bindingClass();
    final String fieldName = fieldDescriptor.fieldName();
    final NumericIndexStrategy indexStrategy;
    final CommonIndexModel indexModel;
    if (Byte.class.isAssignableFrom(bindingClass)) {
      indexStrategy = new SimpleByteIndexStrategy();
      indexModel =
          new BasicIndexModel(
              new NumericDimensionField[] {
                  new BasicNumericDimensionField<>(fieldName, Byte.class)});
    } else if (Short.class.isAssignableFrom(bindingClass)) {
      indexStrategy = new SimpleShortIndexStrategy();
      indexModel =
          new BasicIndexModel(
              new NumericDimensionField[] {
                  new BasicNumericDimensionField<>(fieldName, Short.class)});
    } else if (Integer.class.isAssignableFrom(bindingClass)) {
      indexStrategy = new SimpleIntegerIndexStrategy();
      indexModel =
          new BasicIndexModel(
              new NumericDimensionField[] {
                  new BasicNumericDimensionField<>(fieldName, Integer.class)});
    } else if (Long.class.isAssignableFrom(bindingClass)) {
      indexStrategy = new SimpleLongIndexStrategy();
      indexModel =
          new BasicIndexModel(
              new NumericDimensionField[] {
                  new BasicNumericDimensionField<>(fieldName, Long.class)});
    } else if (Float.class.isAssignableFrom(bindingClass)) {
      indexStrategy = new SimpleFloatIndexStrategy();
      indexModel =
          new BasicIndexModel(
              new NumericDimensionField[] {
                  new BasicNumericDimensionField<>(fieldName, Float.class)});
    } else if (Double.class.isAssignableFrom(bindingClass)) {
      indexStrategy = new SimpleDoubleIndexStrategy();
      indexModel =
          new BasicIndexModel(
              new NumericDimensionField[] {
                  new BasicNumericDimensionField<>(fieldName, Double.class)});
    } else {
      throw new ParameterException(
          "Unsupported numeric attribute index class: " + bindingClass.getName());
    }

    return new AttributeIndexImpl(indexStrategy, indexModel, indexName, fieldName);
  }

}
