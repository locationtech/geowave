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

public class CustomNameIndex extends IndexImpl {
  private String name;

  public CustomNameIndex() {
    super();
  }

  public CustomNameIndex(
      final NumericIndexStrategy indexStrategy,
      final CommonIndexModel indexModel,
      final String name) {
    super(indexStrategy, indexModel);
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public byte[] toBinary() {
    final byte[] selfBinary = super.toBinary();
    final byte[] idBinary = StringUtils.stringToBinary(name);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(selfBinary.length)
                + idBinary.length
                + selfBinary.length);
    VarintUtils.writeUnsignedInt(selfBinary.length, buf);
    buf.put(selfBinary);
    buf.put(idBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int selfBinaryLength = VarintUtils.readUnsignedInt(buf);
    final byte[] selfBinary = ByteArrayUtils.safeRead(buf, selfBinaryLength);

    super.fromBinary(selfBinary);
    final byte[] nameBinary = new byte[buf.remaining()];
    buf.get(nameBinary);
    name = StringUtils.stringFromBinary(nameBinary);
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof CustomNameIndex)) {
      return false;
    }
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
