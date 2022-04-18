/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.field;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.field.ArrayWriter.Encoding;
import org.locationtech.geowave.core.store.util.GenericTypeResolver;

/** This class contains the basic array reader field types */
public class ArrayReader<FieldType> implements FieldReader<FieldType[]> {

  private final FieldReader<FieldType> reader;

  public ArrayReader(final FieldReader<FieldType> reader) {
    this.reader = reader;
  }

  @Override
  public FieldType[] readField(final byte[] fieldData) {
    if ((fieldData == null) || (fieldData.length == 0)) {
      return null;
    }

    final byte encoding = fieldData[0];

    final SerializationHelper<FieldType> serializationHelper =
        new SerializationHelper<FieldType>() {

          @Override
          public int readUnsignedInt(final ByteBuffer buffer) {
            return VarintUtils.readUnsignedInt(buffer);
          }

          @Override
          public FieldType readField(final FieldReader<FieldType> reader, final byte[] bytes) {
            return reader.readField(bytes);
          }
        };

    // try to read the encoding first
    if (encoding == Encoding.FIXED_SIZE_ENCODING.getByteEncoding()) {
      return readFixedSizeField(fieldData, serializationHelper);
    } else if (encoding == Encoding.VARIABLE_SIZE_ENCODING.getByteEncoding()) {
      return readVariableSizeField(fieldData, serializationHelper);
    }

    // class type not supported!
    // to be safe, treat as variable size
    return readVariableSizeField(fieldData, serializationHelper);
  }

  @Override
  public FieldType[] readField(final byte[] fieldData, final byte serializationVersion) {
    if (serializationVersion < FieldUtils.SERIALIZATION_VERSION) {
      final SerializationHelper<FieldType> serializationHelper =
          new SerializationHelper<FieldType>() {
            @Override
            public int readUnsignedInt(final ByteBuffer buffer) {
              return buffer.getInt();
            }

            @Override
            public FieldType readField(final FieldReader<FieldType> reader, final byte[] bytes) {
              return reader.readField(bytes, serializationVersion);
            }
          };

      final byte encoding = fieldData[0];

      // try to read the encoding first
      if (encoding == Encoding.FIXED_SIZE_ENCODING.getByteEncoding()) {
        return readFixedSizeField(fieldData, serializationHelper);
      } else if (encoding == Encoding.VARIABLE_SIZE_ENCODING.getByteEncoding()) {
        return readVariableSizeField(fieldData, serializationHelper);
      }

      // class type not supported!
      // to be safe, treat as variable size
      return readVariableSizeField(fieldData, serializationHelper);
    } else {
      return readField(fieldData);
    }
  }

  @SuppressWarnings("unchecked")
  protected FieldType[] readFixedSizeField(
      final byte[] fieldData,
      final SerializationHelper<FieldType> serializationHelper) {
    if (fieldData.length < 1) {
      return null;
    }

    final List<FieldType> result = new ArrayList<>();

    final ByteBuffer buff = ByteBuffer.wrap(fieldData);

    // this would be bad
    if (buff.get() != Encoding.FIXED_SIZE_ENCODING.getByteEncoding()) {
      return null;
    }

    final int bytesPerEntry = serializationHelper.readUnsignedInt(buff);

    final byte[] data = new byte[Math.min(bytesPerEntry, buff.remaining())];

    while (buff.hasRemaining()) {

      final int header = buff.get();

      for (int i = 0; i < 8; i++) {

        final int mask = (int) Math.pow(2.0, i);

        if ((header & mask) != 0) {
          if (buff.hasRemaining()) {
            buff.get(data);
            result.add(serializationHelper.readField(reader, data));
          } else {
            break;
          }
        } else {
          result.add(null);
        }
      }
    }
    final FieldType[] resultArray =
        (FieldType[]) Array.newInstance(
            GenericTypeResolver.resolveTypeArgument(reader.getClass(), FieldReader.class),
            result.size());
    return result.toArray(resultArray);
  }

  @SuppressWarnings("unchecked")
  protected FieldType[] readVariableSizeField(
      final byte[] fieldData,
      final SerializationHelper<FieldType> serializationHelper) {
    if ((fieldData == null) || (fieldData.length == 0)) {
      return null;
    }
    final List<FieldType> result = new ArrayList<>();

    final ByteBuffer buff = ByteBuffer.wrap(fieldData);

    // this would be bad
    if (buff.get() != Encoding.VARIABLE_SIZE_ENCODING.getByteEncoding()) {
      return null;
    }

    while (buff.remaining() >= 1) {
      final int size = serializationHelper.readUnsignedInt(buff);
      if (size > 0) {
        final byte[] bytes = ByteArrayUtils.safeRead(buff, size);
        result.add(serializationHelper.readField(reader, bytes));
      } else {
        result.add(null);
      }
    }
    final FieldType[] resultArray =
        (FieldType[]) Array.newInstance(
            GenericTypeResolver.resolveTypeArgument(reader.getClass(), FieldReader.class),
            result.size());
    return result.toArray(resultArray);
  }

  private static interface SerializationHelper<FieldType> {
    public int readUnsignedInt(ByteBuffer buffer);

    public FieldType readField(FieldReader<FieldType> reader, byte[] bytes);
  }
}
