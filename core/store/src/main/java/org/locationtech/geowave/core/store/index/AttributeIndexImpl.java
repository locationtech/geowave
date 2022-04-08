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
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.AttributeIndex;

/**
 * Basic implementation of an attribute index.
 */
public class AttributeIndexImpl extends CustomNameIndex implements AttributeIndex {

  private String attributeName;

  public AttributeIndexImpl() {}

  public AttributeIndexImpl(
      final NumericIndexStrategy indexStrategy,
      final CommonIndexModel indexModel,
      final String indexName,
      final String attributeName) {
    super(indexStrategy, indexModel, indexName);
    this.attributeName = attributeName;
  }

  @Override
  public NumericIndexStrategy getIndexStrategy() {
    return indexStrategy;
  }

  @Override
  public CommonIndexModel getIndexModel() {
    return indexModel;
  }

  @Override
  public String getAttributeName() {
    return attributeName;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof AttributeIndexImpl)) {
      return false;
    }
    return super.equals(obj) && attributeName.equals(((AttributeIndexImpl) obj).attributeName);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + super.hashCode();
    result = (prime * result) + attributeName.hashCode();
    return result;
  }

  @Override
  public byte[] toBinary() {
    final byte[] superBinary = super.toBinary();
    final byte[] attributeNameBytes = StringUtils.stringToBinary(attributeName);
    final ByteBuffer buffer =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(superBinary.length)
                + VarintUtils.unsignedIntByteLength(attributeNameBytes.length)
                + superBinary.length
                + attributeNameBytes.length);
    VarintUtils.writeUnsignedInt(superBinary.length, buffer);
    buffer.put(superBinary);
    VarintUtils.writeUnsignedInt(attributeNameBytes.length, buffer);
    buffer.put(attributeNameBytes);
    return buffer.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    final byte[] superBinary = ByteArrayUtils.safeRead(buffer, VarintUtils.readUnsignedInt(buffer));
    final byte[] attributeNameBytes =
        ByteArrayUtils.safeRead(buffer, VarintUtils.readUnsignedInt(buffer));
    super.fromBinary(superBinary);
    attributeName = StringUtils.stringFromBinary(attributeNameBytes);
  }

}
