/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.dimension;

import org.locationtech.geowave.core.store.data.field.ArrayWriter;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class ArrayWrapperWriter<T> implements FieldWriter<Object, ArrayWrapper<T>> {
  protected FieldWriter<Object, T[]> writer;

  protected ArrayWrapperWriter() {
    super();
  }

  public ArrayWrapperWriter(final ArrayWriter<Object, T> writer) {
    this.writer = writer;
  }

  @Override
  public byte[] getVisibility(
      final Object rowValue,
      final String fieldName,
      final ArrayWrapper<T> fieldValue) {
    return fieldValue.getVisibility();
  }

  @Override
  public byte[] writeField(final ArrayWrapper<T> fieldValue) {
    return writer.writeField(fieldValue.getArray());
  }
}
