/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.index;

import java.util.Calendar;
import java.util.Date;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.AttributeIndex;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.AttributeIndexImpl;
import org.locationtech.geowave.core.store.index.AttributeIndexProviderSpi;

/**
 * Provides attribute indices for temporal fields.
 */
public class TemporalAttributeIndexProvider implements AttributeIndexProviderSpi {

  @Override
  public boolean supportsDescriptor(final FieldDescriptor<?> fieldDescriptor) {
    return Calendar.class.isAssignableFrom(fieldDescriptor.bindingClass())
        || Date.class.isAssignableFrom(fieldDescriptor.bindingClass());
  }

  @Override
  public AttributeIndex buildIndex(
      final String indexName,
      final DataTypeAdapter<?> adapter,
      final FieldDescriptor<?> fieldDescriptor) {
    final TemporalOptions options = new TemporalOptions();
    options.setNoTimeRanges(true);
    final Index index = TemporalDimensionalityTypeProvider.createIndexFromOptions(options);
    return new AttributeIndexImpl(
        index.getIndexStrategy(),
        index.getIndexModel(),
        indexName,
        fieldDescriptor.fieldName());
  }

}
