/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store;

public abstract class BaseStoreFactory<T> implements GenericStoreFactory<T> {
  private final String typeName;
  private final String description;
  protected StoreFactoryHelper helper;

  public BaseStoreFactory(
      final String typeName,
      final String description,
      final StoreFactoryHelper helper) {
    super();
    this.typeName = typeName;
    this.description = description;
    this.helper = helper;
  }

  @Override
  public String getType() {
    return typeName;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public StoreFactoryOptions createOptionsInstance() {
    return helper.createOptionsInstance();
  }
}
