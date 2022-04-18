/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.index;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.text.TextIndexEntryConverter;
import org.opengis.feature.simple.SimpleFeature;

public class VectorTextIndexEntryConverter implements TextIndexEntryConverter<SimpleFeature> {
  private int attributeIndex;

  public VectorTextIndexEntryConverter() {
    super();
  }

  public VectorTextIndexEntryConverter(final int attributeIndex) {
    super();
    this.attributeIndex = attributeIndex;
  }

  @Override
  public byte[] toBinary() {
    return VarintUtils.writeUnsignedInt(attributeIndex);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    attributeIndex = VarintUtils.readUnsignedInt(ByteBuffer.wrap(bytes));
  }

  @Override
  public String apply(final SimpleFeature t) {
    return (String) t.getAttribute(attributeIndex);
  }
}
