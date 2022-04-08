/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.Arrays;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class is a wrapper around a byte array to ensure equals and hashcode operations use the
 * values of the bytes rather than explicit object identity
 */
public class ByteArray implements java.io.Serializable, Comparable<ByteArray> {
  private static final long serialVersionUID = 1L;

  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  protected byte[] bytes;

  @SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
  protected transient String string;

  public ByteArray() {
    this(EMPTY_BYTE_ARRAY);
  }

  public ByteArray(final byte[] bytes) {
    this.bytes = bytes;
  }

  public ByteArray(final String string) {
    bytes = StringUtils.stringToBinary(string);
    this.string = string;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public byte[] getNextPrefix() {
    return ByteArrayUtils.getNextPrefix(bytes);
  }

  public String getString() {
    if (string == null) {
      string = StringUtils.stringFromBinary(bytes);
    }
    return string;
  }

  public String getHexString() {
    return ByteArrayUtils.getHexString(bytes);
  }

  @Override
  public String toString() {
    return "ByteArray[" + bytes.length + "]=\"" + getString() + "\"";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + Arrays.hashCode(bytes);
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ByteArray other = (ByteArray) obj;
    return Arrays.equals(bytes, other.bytes);
  }

  public static byte[] toBytes(final ByteArray[] ids) {
    int len = VarintUtils.unsignedIntByteLength(ids.length);
    for (final ByteArray id : ids) {
      len += (id.bytes.length + VarintUtils.unsignedIntByteLength(id.bytes.length));
    }
    final ByteBuffer buffer = ByteBuffer.allocate(len);
    VarintUtils.writeUnsignedInt(ids.length, buffer);
    for (final ByteArray id : ids) {
      VarintUtils.writeUnsignedInt(id.bytes.length, buffer);
      buffer.put(id.bytes);
    }
    return buffer.array();
  }

  public static ByteArray[] fromBytes(final byte[] idData) {
    final ByteBuffer buffer = ByteBuffer.wrap(idData);
    final int len = VarintUtils.readUnsignedInt(buffer);
    ByteArrayUtils.verifyBufferSize(buffer, len);
    final ByteArray[] result = new ByteArray[len];
    for (int i = 0; i < len; i++) {
      final int idSize = VarintUtils.readUnsignedInt(buffer);
      final byte[] id = ByteArrayUtils.safeRead(buffer, idSize);
      result[i] = new ByteArray(id);
    }
    return result;
  }

  @Override
  public int compareTo(final ByteArray o) {
    return ByteArrayUtils.compare(bytes, o.bytes);
  }
}
