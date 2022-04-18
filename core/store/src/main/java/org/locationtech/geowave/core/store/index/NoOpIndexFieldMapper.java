/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.index;

import java.nio.ByteBuffer;
import java.util.List;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;

/**
 * A basic index field mapper that maps an adapter field to an index field of the same class. No
 * transformations are done on the data.
 *
 * @param <I> the index and adapter field type
 */
public class NoOpIndexFieldMapper<I> extends IndexFieldMapper<I, I> {

  private Class<I> indexFieldClass = null;

  public NoOpIndexFieldMapper() {}

  public NoOpIndexFieldMapper(final Class<I> indexFieldClass) {
    this.indexFieldClass = indexFieldClass;
  }

  @Override
  protected void initFromOptions(
      List<FieldDescriptor<I>> inputFieldDescriptors,
      IndexFieldOptions options) {}

  @Override
  public I toIndex(List<I> nativeFieldValues) {
    return nativeFieldValues.get(0);
  }

  @Override
  public void toAdapter(final I indexFieldValue, final RowBuilder<?> rowBuilder) {
    rowBuilder.setField(adapterFields[0], indexFieldValue);
  }

  @Override
  public Class<I> indexFieldType() {
    return indexFieldClass;
  }

  @Override
  public Class<I> adapterFieldType() {
    return indexFieldClass;
  }

  @Override
  public short adapterFieldCount() {
    return 1;
  }

  private byte[] classBytes = null;

  @Override
  protected int byteLength() {
    classBytes = StringUtils.stringToBinary(indexFieldClass.getName());
    return super.byteLength()
        + VarintUtils.unsignedShortByteLength((short) classBytes.length)
        + classBytes.length;
  }

  @Override
  protected void writeBytes(final ByteBuffer buffer) {
    VarintUtils.writeUnsignedShort((short) classBytes.length, buffer);
    buffer.put(classBytes);
    super.writeBytes(buffer);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  protected void readBytes(final ByteBuffer buffer) {
    classBytes = new byte[VarintUtils.readUnsignedShort(buffer)];
    buffer.get(classBytes);
    try {
      indexFieldClass = (Class) Class.forName(StringUtils.stringFromBinary(classBytes));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Unable to find class for no-op index field mapper.");
    }
    super.readBytes(buffer);
  }

}
