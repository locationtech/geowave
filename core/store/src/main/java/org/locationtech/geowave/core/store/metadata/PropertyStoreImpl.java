/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.metadata;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.DataStoreProperty;
import org.locationtech.geowave.core.store.PropertyStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataType;

public class PropertyStoreImpl extends AbstractGeoWavePersistence<DataStoreProperty> implements
    PropertyStore {

  public PropertyStoreImpl(final DataStoreOperations operations, final DataStoreOptions options) {
    super(operations, options, MetadataType.STORE_PROPERTIES);
  }

  private ByteArray keyToPrimaryId(final String key) {
    return new ByteArray(StringUtils.stringToBinary(key));
  }

  @Override
  public DataStoreProperty getProperty(final String propertyKey) {
    return internalGetObject(keyToPrimaryId(propertyKey), null, false);
  }

  @Override
  public void setProperty(final DataStoreProperty property) {
    final ByteArray primaryId = getPrimaryId(property);
    if (objectExists(primaryId, null)) {
      remove(primaryId);
    }
    addObject(property);
  }

  @Override
  protected ByteArray getPrimaryId(final DataStoreProperty persistedObject) {
    return keyToPrimaryId(persistedObject.getKey());
  }


}
