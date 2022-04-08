/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.index;

import java.util.function.Supplier;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;

/**
 * A base interface for registering new index field mappers with GeoWave via SPI.
 */
public interface IndexFieldMapperRegistrySPI {

  /**
   * @return a list of index field mappers to register
   */
  RegisteredFieldMapper[] getRegisteredFieldMappers();


  /**
   * A registered field mapper contains the constructor for the field mapper and a persistable ID.
   */
  public static class RegisteredFieldMapper {
    private final Supplier<? extends IndexFieldMapper<?, ?>> constructor;
    private final short persistableId;

    public RegisteredFieldMapper(
        final Supplier<? extends IndexFieldMapper<?, ?>> constructor,
        final short persistableId) {
      this.constructor = constructor;
      this.persistableId = persistableId;
    }

    @SuppressWarnings("unchecked")
    public Supplier<IndexFieldMapper<?, ?>> getConstructor() {
      return (Supplier<IndexFieldMapper<?, ?>>) constructor;
    }

    public short getPersistableId() {
      return persistableId;
    }
  }

}
