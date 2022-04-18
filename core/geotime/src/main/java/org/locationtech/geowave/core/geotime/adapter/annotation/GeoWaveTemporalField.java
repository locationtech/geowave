/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.adapter.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveFieldAnnotation;

/**
 * Annotation for temporal GeoWave fields for the {@link BasicDataTypeAdapter}. This annotation
 * allows temporal index hints to be easily defined.
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@GeoWaveFieldAnnotation(fieldDescriptorBuilder = TemporalAnnotatedFieldDescriptorBuilder.class)
public @interface GeoWaveTemporalField {
  /**
   * The name to use for the field.
   */
  String name() default "";

  /**
   * Index hints to use for the field.
   */
  String[] indexHints() default {};

  /**
   * If {@code true} this field will be preferred for temporal indices and treated as a time
   * instant.
   */
  boolean startTimeIndexHint() default false;

  /**
   * If {@code true} this field will be preferred for temporal indices and treated as the start
   * time.
   */
  boolean endTimeIndexHint() default false;

  /**
   * If {@code true} this field will be preferred for temporal indices and treated as the end time.
   */
  boolean timeIndexHint() default false;
}
