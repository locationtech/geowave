/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.util.Map;
import org.locationtech.geowave.core.store.api.RowBuilder;

public class SingletonFieldRowBuilder<T> implements RowBuilder<T> {
  private T fieldValue;

  @SuppressWarnings("unchecked")
  @Override
  public void setField(final String fieldName, final Object fieldValue) {
    this.fieldValue = (T) fieldValue;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setFields(final Map<String, Object> values) {
    if (!values.isEmpty()) {
      this.fieldValue = (T) values.values().iterator().next();
    }
  }

  @Override
  public T buildRow(final byte[] dataId) {
    return fieldValue;
  }

}
