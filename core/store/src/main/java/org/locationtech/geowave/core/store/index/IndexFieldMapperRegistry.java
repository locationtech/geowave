/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.index;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.locationtech.geowave.core.index.SPIServiceRegistry;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi.PersistableIdAndConstructor;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;
import org.locationtech.geowave.core.store.index.IndexFieldMapperRegistrySPI.RegisteredFieldMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Uses SPI to find registered adapter to index field mappers.
 */
public class IndexFieldMapperRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexFieldMapperRegistry.class);

  private static IndexFieldMapperRegistry INSTANCE = null;

  private Map<Class<?>, List<RegisteredFieldMapper>> indexFieldMappings = Maps.newHashMap();
  private final int totalFieldMappings;

  private IndexFieldMapperRegistry() {
    final Iterator<IndexFieldMapperRegistrySPI> spiIter =
        new SPIServiceRegistry(IndexFieldMapperRegistry.class).load(
            IndexFieldMapperRegistrySPI.class);
    int mappingCount = 0;
    while (spiIter.hasNext()) {
      final IndexFieldMapperRegistrySPI providedFieldMappers = spiIter.next();
      for (RegisteredFieldMapper registeredMapper : providedFieldMappers.getRegisteredFieldMappers()) {
        Class<?> indexFieldType = registeredMapper.getConstructor().get().indexFieldType();
        if (!indexFieldMappings.containsKey(indexFieldType)) {
          indexFieldMappings.put(indexFieldType, Lists.newArrayList());
        }
        indexFieldMappings.get(indexFieldType).add(registeredMapper);
        mappingCount++;
      }
    }
    this.totalFieldMappings = mappingCount;
  }

  public static IndexFieldMapperRegistry instance() {
    if (INSTANCE == null) {
      INSTANCE = new IndexFieldMapperRegistry();
    }
    return INSTANCE;
  }

  @SuppressWarnings("unchecked")
  public PersistableIdAndConstructor[] getPersistables() {
    final Collection<List<RegisteredFieldMapper>> registeredFieldMappers =
        indexFieldMappings.values();
    final PersistableIdAndConstructor[] persistables =
        new PersistableIdAndConstructor[totalFieldMappings];
    int persistableIndex = 0;
    for (final List<RegisteredFieldMapper> mappers : registeredFieldMappers) {
      for (final RegisteredFieldMapper mapper : mappers) {
        persistables[persistableIndex++] =
            new PersistableIdAndConstructor(
                mapper.getPersistableId(),
                (Supplier<Persistable>) (Supplier<?>) mapper.getConstructor());
      }
    }
    return persistables;
  };

  /**
   * Returns all field mappers that are available for the given index field class.
   * 
   * @param indexFieldClass the index field class
   * @return a list of available mappers
   */
  public List<IndexFieldMapper<?, ?>> getAvailableMappers(final Class<?> indexFieldClass) {
    List<RegisteredFieldMapper> registeredMappers = indexFieldMappings.get(indexFieldClass);
    List<IndexFieldMapper<?, ?>> fieldMappers =
        Lists.newArrayListWithCapacity(
            registeredMappers != null ? registeredMappers.size() + 1 : 1);
    if (registeredMappers != null) {
      registeredMappers.forEach(mapper -> fieldMappers.add(mapper.getConstructor().get()));
    }
    fieldMappers.add(new NoOpIndexFieldMapper<>(indexFieldClass));
    return fieldMappers;
  }
}
