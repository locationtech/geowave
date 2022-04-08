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
import java.util.EnumSet;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.index.text.CaseSensitivity;
import org.locationtech.geowave.core.index.text.TextIndexEntryConverter;
import org.locationtech.geowave.core.index.text.TextIndexStrategy;
import org.locationtech.geowave.core.index.text.TextSearchType;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.AttributeIndex;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * Provides attribute indices for string fields.
 */
public class TextAttributeIndexProvider implements AttributeIndexProviderSpi {

  @Override
  public boolean supportsDescriptor(final FieldDescriptor<?> fieldDescriptor) {
    return String.class.isAssignableFrom(fieldDescriptor.bindingClass());
  }

  @Override
  public AttributeIndex buildIndex(
      final String indexName,
      final DataTypeAdapter<?> adapter,
      final FieldDescriptor<?> fieldDescriptor) {
    return new CustomAttributeIndex<>(
        new TextIndexStrategy<>(
            EnumSet.of(
                TextSearchType.BEGINS_WITH,
                TextSearchType.ENDS_WITH,
                TextSearchType.EXACT_MATCH),
            EnumSet.of(CaseSensitivity.CASE_SENSITIVE, CaseSensitivity.CASE_INSENSITIVE),
            new AdapterFieldTextIndexEntryConverter<>(adapter, fieldDescriptor.fieldName())),
        indexName,
        fieldDescriptor.fieldName());
  }

  /**
   * A converter that pulls the string value to be indexed from a specific field of the entry using
   * the data adapter that the entry belongs to.
   *
   * @param <T> the type of each entry and the adapter
   */
  public static class AdapterFieldTextIndexEntryConverter<T> implements TextIndexEntryConverter<T> {

    private DataTypeAdapter<T> adapter;
    private String fieldName;

    public AdapterFieldTextIndexEntryConverter() {}

    public AdapterFieldTextIndexEntryConverter(
        final DataTypeAdapter<T> adapter,
        final String fieldName) {
      this.adapter = adapter;
      this.fieldName = fieldName;
    }

    public String getFieldName() {
      return fieldName;
    }

    public DataTypeAdapter<T> getAdapter() {
      return adapter;
    }

    @Override
    public String apply(final T t) {
      return (String) adapter.getFieldValue(t, fieldName);
    }

    @Override
    public byte[] toBinary() {
      final byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
      final byte[] fieldNameBytes = StringUtils.stringToBinary(fieldName);
      final ByteBuffer buffer =
          ByteBuffer.allocate(
              VarintUtils.unsignedIntByteLength(adapterBytes.length)
                  + VarintUtils.unsignedIntByteLength(fieldNameBytes.length)
                  + adapterBytes.length
                  + fieldNameBytes.length);
      VarintUtils.writeUnsignedInt(adapterBytes.length, buffer);
      buffer.put(adapterBytes);
      VarintUtils.writeUnsignedInt(fieldNameBytes.length, buffer);
      buffer.put(fieldNameBytes);
      return buffer.array();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buffer = ByteBuffer.wrap(bytes);
      final byte[] adapterBytes =
          ByteArrayUtils.safeRead(buffer, VarintUtils.readUnsignedInt(buffer));
      adapter = (DataTypeAdapter) PersistenceUtils.fromBinary(adapterBytes);
      final byte[] fieldNameBytes =
          ByteArrayUtils.safeRead(buffer, VarintUtils.readUnsignedInt(buffer));
      fieldName = StringUtils.stringFromBinary(fieldNameBytes);
    }

  }

}
