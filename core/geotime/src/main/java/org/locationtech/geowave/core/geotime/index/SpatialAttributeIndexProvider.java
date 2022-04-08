/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.index;

import org.locationtech.geowave.core.geotime.adapter.SpatialFieldDescriptor;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.AttributeIndex;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.AttributeIndexImpl;
import org.locationtech.geowave.core.store.index.AttributeIndexProviderSpi;
import org.locationtech.jts.geom.Geometry;

/**
 * Provides attribute indices for spatial fields.
 */
public class SpatialAttributeIndexProvider implements AttributeIndexProviderSpi {

  @Override
  public boolean supportsDescriptor(final FieldDescriptor<?> fieldDescriptor) {
    return Geometry.class.isAssignableFrom(fieldDescriptor.bindingClass());
  }

  @Override
  public AttributeIndex buildIndex(
      final String indexName,
      final DataTypeAdapter<?> adapter,
      final FieldDescriptor<?> fieldDescriptor) {
    final SpatialOptions options = new SpatialOptions();
    if (fieldDescriptor instanceof SpatialFieldDescriptor) {
      options.setCrs(GeometryUtils.getCrsCode(((SpatialFieldDescriptor<?>) fieldDescriptor).crs()));
    }
    final Index index = SpatialDimensionalityTypeProvider.createIndexFromOptions(options);
    return new AttributeIndexImpl(
        index.getIndexStrategy(),
        index.getIndexModel(),
        indexName,
        fieldDescriptor.fieldName());
  }

}
