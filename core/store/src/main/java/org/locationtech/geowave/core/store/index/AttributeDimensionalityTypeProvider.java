/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.index;

import java.util.ServiceLoader;
import javax.annotation.Nullable;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.AttributeIndex;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.spi.DimensionalityTypeProviderSpi;
import com.beust.jcommander.ParameterException;

/**
 * Provides an attribute index for any field that supports them.
 */
public class AttributeDimensionalityTypeProvider implements
    DimensionalityTypeProviderSpi<AttributeIndexOptions> {

  private static ServiceLoader<AttributeIndexProviderSpi> serviceLoader = null;

  public AttributeDimensionalityTypeProvider() {}

  @Override
  public String getDimensionalityTypeName() {
    return "attribute";
  }

  @Override
  public String getDimensionalityTypeDescription() {
    return "This index type can be used to index any attribute of a type that supports indexing.";
  }

  @Override
  public AttributeIndexOptions createOptions() {
    return new AttributeIndexOptions();
  }

  @Override
  public Index createIndex(final DataStore dataStore, final AttributeIndexOptions options) {
    return createIndexFromOptions(dataStore, options);
  }

  public static Index createIndexFromOptions(
      final DataStore dataStore,
      final AttributeIndexOptions options) {
    if ((options.getTypeName() == null) || (options.getTypeName().length() == 0)) {
      throw new ParameterException(
          "A type name must be specified when creating an attribute index.");
    }
    if ((options.getAttributeName() == null) || (options.getAttributeName().length() == 0)) {
      throw new ParameterException(
          "An attribute name must be specified when creating an attribute index.");
    }
    final DataTypeAdapter<?> adapter = dataStore.getType(options.getTypeName());
    if (adapter == null) {
      throw new ParameterException(
          "A type with name '" + options.getTypeName() + "' could not be found in the data store.");
    }
    final FieldDescriptor<?> descriptor = adapter.getFieldDescriptor(options.getAttributeName());
    if (descriptor == null) {
      throw new ParameterException(
          "An attribute with name '"
              + options.getAttributeName()
              + "' could not be found in the type.");
    }
    return createIndexForDescriptor(adapter, descriptor, options.getIndexName());
  }

  public static Index createIndexForDescriptor(
      final DataTypeAdapter<?> adapter,
      final FieldDescriptor<?> descriptor,
      final @Nullable String indexName) {
    if (serviceLoader == null) {
      serviceLoader = ServiceLoader.load(AttributeIndexProviderSpi.class);
    }
    for (final AttributeIndexProviderSpi indexProvider : serviceLoader) {
      if (indexProvider.supportsDescriptor(descriptor)) {
        return indexProvider.buildIndex(
            indexName == null
                ? AttributeIndex.defaultAttributeIndexName(
                    adapter.getTypeName(),
                    descriptor.fieldName())
                : indexName,
            adapter,
            descriptor);
      }
    }

    throw new ParameterException(
        "No attribute index implementations were found for the field type: "
            + descriptor.bindingClass().getName());
  }



}
