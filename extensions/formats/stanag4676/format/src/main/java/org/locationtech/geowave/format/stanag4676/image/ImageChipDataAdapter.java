/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.stanag4676.image;

import java.util.Map;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.FieldDescriptorBuilder;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class ImageChipDataAdapter implements DataTypeAdapter<ImageChip> {
  public static final String ADAPTER_TYPE_NAME = "image";
  private static final String IMAGE_FIELD_NAME = "image";
  private static final FieldDescriptor<byte[]> IMAGE_FIELD =
      new FieldDescriptorBuilder<>(byte[].class).fieldName(IMAGE_FIELD_NAME).build();
  private static final FieldDescriptor<?>[] FIELDS = new FieldDescriptor[] {IMAGE_FIELD};

  public ImageChipDataAdapter() {
    super();
  }

  @Override
  public String getTypeName() {
    return ADAPTER_TYPE_NAME;
  }

  @Override
  public byte[] getDataId(final ImageChip entry) {
    return entry.getDataId();
  }

  @Override
  public FieldReader<Object> getReader(final String fieldId) {
    if (IMAGE_FIELD_NAME.equals(fieldId)) {
      return (FieldReader) FieldUtils.getDefaultReaderForClass(byte[].class);
    }
    return null;
  }

  @Override
  public byte[] toBinary() {
    return new byte[] {};
  }

  @Override
  public void fromBinary(final byte[] bytes) {}

  @Override
  public FieldWriter<Object> getWriter(final String fieldId) {
    if (IMAGE_FIELD_NAME.equals(fieldId)) {
      return (FieldWriter) FieldUtils.getDefaultWriterForClass(byte[].class);
    }
    return null;
  }

  @Override
  public Object getFieldValue(final ImageChip entry, final String fieldName) {
    return entry.getImageBinary();
  }

  @Override
  public Class<ImageChip> getDataClass() {
    return ImageChip.class;
  }

  @Override
  public RowBuilder<ImageChip> newRowBuilder(final FieldDescriptor<?>[] outputFieldDescriptors) {
    return new ImageChipRowBuilder();
  }

  @Override
  public FieldDescriptor<?>[] getFieldDescriptors() {
    return FIELDS;
  }

  @Override
  public FieldDescriptor<?> getFieldDescriptor(final String fieldName) {
    return IMAGE_FIELD;
  }

  private static class ImageChipRowBuilder implements RowBuilder<ImageChip> {
    private byte[] imageData;

    @Override
    public void setField(final String fieldName, final Object fieldValue) {
      if (fieldValue instanceof byte[]) {
        imageData = (byte[]) fieldValue;
      }
    }

    @Override
    public void setFields(final Map<String, Object> values) {
      values.entrySet().forEach((e) -> setField(e.getKey(), e.getValue()));
    }

    @Override
    public ImageChip buildRow(final byte[] dataId) {
      return ImageChipUtils.fromDataIdAndValue(dataId, imageData);
    }

  }
}
