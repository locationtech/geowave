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
import org.locationtech.geowave.core.index.CustomIndexStrategy;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.AttributeIndex;

/**
 * An implementation of {@link CustomIndex} that supports attribute indices. This can be used to
 * create attribute indices on non-numeric fields.
 *
 * @param <E> The entry type (such as SimpleFeature, GridCoverage, or whatever type the adapter
 *        uses)
 * @param <C> The custom constraints type can be any arbitrary type, although should be persistable
 *        so that it can work outside of just client code (such as server-side filtering,
 *        map-reduce, or spark)
 */
public class CustomAttributeIndex<E, C extends Persistable> extends CustomIndex<E, C> implements
    AttributeIndex {

  private String attributeName;

  public CustomAttributeIndex() {
    super();
  }

  public CustomAttributeIndex(
      final CustomIndexStrategy<E, C> indexStrategy,
      final String id,
      final String attributeName) {
    super(indexStrategy, id);
    this.attributeName = attributeName;
  }

  @Override
  public String getAttributeName() {
    return attributeName;
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
    final CustomAttributeIndex<?, ?> other = (CustomAttributeIndex<?, ?>) obj;
    return super.equals(obj) && attributeName.equals(other.attributeName);
  }

  @Override
  public byte[] toBinary() {
    final byte[] baseBinary = super.toBinary();
    final byte[] attributeNameBytes = StringUtils.stringToBinary(attributeName);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(baseBinary.length)
                + VarintUtils.unsignedIntByteLength(attributeNameBytes.length)
                + baseBinary.length
                + attributeNameBytes.length);
    VarintUtils.writeUnsignedInt(attributeNameBytes.length, buf);
    buf.put(attributeNameBytes);
    VarintUtils.writeUnsignedInt(baseBinary.length, buf);
    buf.put(baseBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final byte[] attributeNameBytes =
        ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
    attributeName = StringUtils.stringFromBinary(attributeNameBytes);
    final byte[] baseBinary = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
    super.fromBinary(baseBinary);
  }

}
