/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.binning;

import java.nio.ByteBuffer;
import java.util.OptionalInt;
import org.locationtech.geowave.core.index.VarintUtils;

/** A single-field value strategy that declares a fixed bin length, as epoch or H3 bins have. */
public class FixedLengthTestBinningStrategy extends FieldValueBinningStrategy {
  private int length;

  public FixedLengthTestBinningStrategy() {
    super();
  }

  public FixedLengthTestBinningStrategy(final int length, final String field) {
    super(field);
    this.length = length;
  }

  @Override
  public String getStrategyName() {
    return "FIXED_LENGTH_TEST";
  }

  @Override
  public OptionalInt getFixedBinLength() {
    return OptionalInt.of(length);
  }

  @Override
  public byte[] toBinary() {
    final byte[] parentBinary = super.toBinary();
    final ByteBuffer buffer =
        ByteBuffer.allocate(VarintUtils.unsignedIntByteLength(length) + parentBinary.length);
    VarintUtils.writeUnsignedInt(length, buffer);
    buffer.put(parentBinary);
    return buffer.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    length = VarintUtils.readUnsignedInt(buffer);
    final byte[] parentBinary = new byte[buffer.remaining()];
    buffer.get(parentBinary);
    super.fromBinary(parentBinary);
  }
}
