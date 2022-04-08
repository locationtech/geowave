/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistableFactory;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.data.field.PersistableReader;
import org.locationtech.geowave.core.store.data.field.PersistableWriter;

abstract public class SimpleAbstractDataAdapter<T extends Persistable> implements
    DataTypeAdapter<T> {
  protected static final String SINGLETON_FIELD_NAME = "FIELD";
  protected FieldDescriptor<T> singletonFieldDescriptor;
  private FieldReader<Object> reader = null;
  private FieldWriter<Object> writer = null;

  public SimpleAbstractDataAdapter() {
    super();
    singletonFieldDescriptor =
        new FieldDescriptorBuilder<>(getDataClass()).fieldName(SINGLETON_FIELD_NAME).build();
  }

  @Override
  public byte[] toBinary() {
    return new byte[0];
  }

  @Override
  public void fromBinary(final byte[] bytes) {}

  @Override
  public Object getFieldValue(final T entry, final String fieldName) {
    return entry;
  }

  @Override
  public RowBuilder<T> newRowBuilder(final FieldDescriptor<?>[] outputFieldDescriptors) {
    return new SingletonFieldRowBuilder<T>();
  }

  @Override
  public FieldDescriptor<?>[] getFieldDescriptors() {
    return new FieldDescriptor<?>[] {singletonFieldDescriptor};
  }

  @Override
  public FieldDescriptor<?> getFieldDescriptor(final String fieldName) {
    return singletonFieldDescriptor;
  }

  @Override
  public FieldWriter<Object> getWriter(final String fieldName) {
    if (writer == null) {
      writer = new PersistableWriter();
    }
    return writer;
  }

  @Override
  public FieldReader<Object> getReader(final String fieldName) {
    if (reader == null) {
      reader =
          new PersistableReader(
              PersistableFactory.getInstance().getClassIdMapping().get(getDataClass()));
    }
    return reader;
  }
}
