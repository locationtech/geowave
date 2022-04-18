/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.options;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;

public class FilterByTypeQueryOptions<T> implements DataTypeQueryOptions<T> {
  private String[] typeNames;
  private String[] fieldNames;

  public FilterByTypeQueryOptions() {}

  public FilterByTypeQueryOptions(final String[] typeNames) {
    this.typeNames = typeNames;
  }

  public FilterByTypeQueryOptions(final String typeName, final String... fieldNames) {
    super();
    typeNames = new String[] {typeName};
    this.fieldNames = ((fieldNames != null) && (fieldNames.length == 0)) ? null : fieldNames;
  }

  @Override
  public String[] getTypeNames() {
    return typeNames;
  }

  public String[] getFieldNames() {
    return fieldNames;
  }

  @Override
  public byte[] toBinary() {
    byte[] typeNamesBinary, fieldNamesBinary;
    if ((typeNames != null) && (typeNames.length > 0)) {
      typeNamesBinary = StringUtils.stringsToBinary(typeNames);
    } else {
      typeNamesBinary = new byte[0];
    }
    if ((fieldNames != null) && (fieldNames.length > 0)) {
      fieldNamesBinary = StringUtils.stringsToBinary(fieldNames);
    } else {
      fieldNamesBinary = new byte[0];
    }
    final ByteBuffer buf =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(typeNamesBinary.length)
                + fieldNamesBinary.length
                + typeNamesBinary.length);
    VarintUtils.writeUnsignedInt(typeNamesBinary.length, buf);
    buf.put(typeNamesBinary);
    buf.put(fieldNamesBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int typeNamesBytesLength = VarintUtils.readUnsignedInt(buf);
    if (typeNamesBytesLength <= 0) {
      typeNames = new String[0];
    } else {
      final byte[] typeNamesBytes = ByteArrayUtils.safeRead(buf, typeNamesBytesLength);
      typeNames = StringUtils.stringsFromBinary(typeNamesBytes);
    }
    final byte[] fieldNamesBytes = new byte[buf.remaining()];
    if (fieldNamesBytes.length == 0) {
      fieldNames = null;
    } else {
      buf.get(fieldNamesBytes);
      fieldNames = StringUtils.stringsFromBinary(fieldNamesBytes);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + Arrays.hashCode(fieldNames);
    result = (prime * result) + Arrays.hashCode(typeNames);
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
    final FilterByTypeQueryOptions other = (FilterByTypeQueryOptions) obj;
    if (!Arrays.equals(fieldNames, other.fieldNames)) {
      return false;
    }
    if (!Arrays.equals(typeNames, other.typeNames)) {
      return false;
    }
    return true;
  }
}
