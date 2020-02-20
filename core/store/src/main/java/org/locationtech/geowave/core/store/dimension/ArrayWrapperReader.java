/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.dimension;

import org.locationtech.geowave.core.store.data.field.ArrayReader;
import org.locationtech.geowave.core.store.data.field.FieldReader;

public class ArrayWrapperReader<T> implements FieldReader<ArrayWrapper<T>> {

  protected FieldReader<T[]> reader;

  protected ArrayWrapperReader() {
    super();
  }

  public ArrayWrapperReader(final ArrayReader<T> reader) {
    this.reader = reader;
  }

  @Override
  public ArrayWrapper<T> readField(final byte[] fieldData) {
    return new ArrayWrapper<>(reader.readField(fieldData));
  }
}
