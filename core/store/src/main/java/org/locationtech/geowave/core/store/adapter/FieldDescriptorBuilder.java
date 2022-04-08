/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.util.Set;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import com.google.common.collect.Sets;

/**
 * A builder for adapter field descriptors.
 *
 * @param <T> the adapter field type
 * @param <F> the field descriptor class
 * @param <B> the builder class
 */
public class FieldDescriptorBuilder<T, F extends FieldDescriptor<T>, B extends FieldDescriptorBuilder<T, F, B>> {
  protected final Class<T> bindingClass;
  protected String fieldName;
  protected Set<IndexDimensionHint> indexHints = Sets.newHashSet();

  /**
   * Create a new `FeatureDescriptorBuilder` for a field of the given type.
   * 
   * @param bindingClass the adapter field type
   */
  public FieldDescriptorBuilder(final Class<T> bindingClass) {
    this.bindingClass = bindingClass;
  }

  /**
   * Supply a field name for the field.
   * 
   * @param fieldName the name of the field
   * @return this builder
   */
  public B fieldName(final String fieldName) {
    this.fieldName = fieldName;
    return (B) this;
  }

  /**
   * Add an index hint to the field. Index hints are used by GeoWave to determine how an adapter
   * should be mapped to an index.
   * 
   * @param hint the index hint to set
   * @return this builder
   */
  public B indexHint(final IndexDimensionHint hint) {
    this.indexHints.add(hint);
    return (B) this;
  }

  /**
   * Builds the field descriptor.
   * 
   * @return the field descriptor
   */
  public F build() {
    return (F) new BaseFieldDescriptor<>(bindingClass, fieldName, indexHints);
  }
}
