/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.index;

import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.AttributeIndex;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * SPI interface for supporting new attribute indices. Implementing this interface can allow the
 * creation of attribute indices for field types that are not supported by core GeoWave.
 */
public interface AttributeIndexProviderSpi {

  /**
   * Determines if the supplied field descriptor is supported by this attribute index provider.
   *
   * @param fieldDescriptor the descriptor to check
   * @return {@code true} if this provider can create an attribute index for the descriptor
   */
  boolean supportsDescriptor(FieldDescriptor<?> fieldDescriptor);

  /**
   * Creates an attribute index for the given descriptor.
   *
   * @param indexName the name of the attribute index
   * @param adapter the adapter that the field descriptor belongs to
   * @param fieldDescriptor the field descriptor to create an index for
   * @return the attribute index
   */
  AttributeIndex buildIndex(
      String indexName,
      DataTypeAdapter<?> adapter,
      FieldDescriptor<?> fieldDescriptor);

}
