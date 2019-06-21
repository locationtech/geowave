/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.field;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractMainParam<T> implements RestFieldValue<T> {
  protected final int ordinal;
  protected final int totalMainParams;
  protected final Field listMainParamField;
  protected final Object instance;
  protected final RestField<T> delegateField;

  public AbstractMainParam(
      final int ordinal,
      final int totalMainParams,
      final Field listMainParamField,
      final RestField<T> delegateField,
      final Object instance) {
    this.ordinal = ordinal;
    this.totalMainParams = totalMainParams;
    this.listMainParamField = listMainParamField;
    this.delegateField = delegateField;
    this.instance = instance;
  }

  @Override
  public String getName() {
    return delegateField.getName();
  }

  @Override
  public Class<T> getType() {
    return delegateField.getType();
  }

  @Override
  public String getDescription() {
    return delegateField.getDescription();
  }

  @Override
  public boolean isRequired() {
    return delegateField.isRequired();
  }

  @Override
  public void setValue(final T value) throws IllegalArgumentException, IllegalAccessException {
    // HP Fortify "Access Control" false positive
    // The need to change the accessibility here is
    // necessary, has been review and judged to be safe
    listMainParamField.setAccessible(true);
    List<String> currentValue = (List<String>) listMainParamField.get(instance);
    if ((currentValue == null) || (currentValue.size() == 0)) {
      currentValue = new ArrayList<>(totalMainParams);
      for (int i = 0; i < totalMainParams; i++) {
        currentValue.add("");
      }
      listMainParamField.set(instance, currentValue);
    }

    currentValue.set(ordinal, valueToString(value));
  }

  protected abstract String valueToString(T value);

  @Override
  public Field getField() {
    return this.listMainParamField;
  }
}
