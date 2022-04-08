/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.field;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.VarintUtils;

/** This class contains the basic object array writer field types */
public abstract class ArrayWriter<FieldType> implements FieldWriter<FieldType[]> {
  public static enum Encoding {
    FIXED_SIZE_ENCODING((byte) 0), VARIABLE_SIZE_ENCODING((byte) 1);

    private final byte encoding;

    Encoding(final byte encoding) {
      this.encoding = encoding;
    }

    public byte getByteEncoding() {
      return encoding;
    }
  }

  private final FieldWriter<FieldType> writer;

  public ArrayWriter(final FieldWriter<FieldType> writer) {
    this.writer = writer;
  }

  protected byte[] writeFixedSizeField(final FieldType[] fieldValue) {

    if (fieldValue == null) {
      return new byte[] {};
    }

    final byte[][] byteData = getBytes(fieldValue);

    int bytesPerEntry = 0;
    for (final byte[] bytes : byteData) {
      if (bytes.length > 0) {
        bytesPerEntry = bytes.length;
      }
    }

    final ByteBuffer buf =
        ByteBuffer.allocate(
            1
                + VarintUtils.unsignedIntByteLength(bytesPerEntry)
                + (int) Math.ceil(fieldValue.length / 8.0)
                + getLength(byteData));

    // this is a header value to indicate how data should be read/written
    buf.put(Encoding.FIXED_SIZE_ENCODING.getByteEncoding());

    // this is a header value to indicate the size of each entry
    VarintUtils.writeUnsignedInt(bytesPerEntry, buf);

    for (int i = 0; i < fieldValue.length; i += 8) {

      int header = 255;

      final int headerIdx = buf.position();
      buf.position(headerIdx + 1);

      for (int j = 0; ((i + j) < fieldValue.length) && (j < 8); j++) {
        final int mask = ~((int) Math.pow(2.0, j));
        if (fieldValue[i + j] == null) {
          header = header & mask;
        } else {
          buf.put(byteData[i + j]);
        }
      }

      buf.put(headerIdx, (byte) header);
    }

    return buf.array();
  }

  protected byte[] writeVariableSizeField(final FieldType[] fieldValue) {
    if (fieldValue == null) {
      return new byte[] {};
    }

    final byte[][] bytes = getBytes(fieldValue);

    int sizeBytes = 0;
    for (final byte[] entry : bytes) {
      sizeBytes += VarintUtils.unsignedIntByteLength(entry.length);
    }

    final ByteBuffer buf = ByteBuffer.allocate(1 + sizeBytes + getLength(bytes));

    // this is a header value to indicate how data should be read/written
    buf.put(Encoding.VARIABLE_SIZE_ENCODING.getByteEncoding());

    for (final byte[] entry : bytes) {
      VarintUtils.writeUnsignedInt(entry.length, buf);
      if (entry.length > 0) {
        buf.put(entry);
      }
    }

    return buf.array();
  }

  private byte[][] getBytes(final FieldType[] fieldData) {

    final byte[][] bytes = new byte[fieldData.length][];
    for (int i = 0; i < fieldData.length; i++) {
      if (fieldData[i] == null) {
        bytes[i] = new byte[] {};
      } else {
        bytes[i] = writer.writeField(fieldData[i]);
      }
    }
    return bytes;
  }

  private int getLength(final byte[][] bytes) {
    int length = 0;
    for (final byte[] entry : bytes) {
      length += entry.length;
    }
    return length;
  }

  public static class FixedSizeObjectArrayWriter<FieldType> extends ArrayWriter<FieldType> {
    public FixedSizeObjectArrayWriter(final FieldWriter<FieldType> writer) {
      super(writer);
    }

    @Override
    public byte[] writeField(final FieldType[] fieldValue) {
      return super.writeFixedSizeField(fieldValue);
    }
  }

  public static class VariableSizeObjectArrayWriter<FieldType> extends ArrayWriter<FieldType> {
    public VariableSizeObjectArrayWriter(final FieldWriter<FieldType> writer) {
      super(writer);
    }

    @Override
    public byte[] writeField(final FieldType[] fieldValue) {
      return super.writeVariableSizeField(fieldValue);
    }
  }
}
