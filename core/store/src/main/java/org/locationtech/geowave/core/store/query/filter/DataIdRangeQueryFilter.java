/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import com.google.common.primitives.UnsignedBytes;

public class DataIdRangeQueryFilter implements QueryFilter {
  private byte[] startDataIdInclusive;
  private byte[] endDataIdInclusive;

  public DataIdRangeQueryFilter() {}

  public DataIdRangeQueryFilter(
      final byte[] startDataIdInclusive,
      final byte[] endDataIdInclusive) {
    this.startDataIdInclusive = startDataIdInclusive;
    this.endDataIdInclusive = endDataIdInclusive;
  }

  @Override
  public boolean accept(
      final CommonIndexModel indexModel,
      final IndexedPersistenceEncoding persistenceEncoding) {
    return ((startDataIdInclusive == null)
        || (UnsignedBytes.lexicographicalComparator().compare(
            startDataIdInclusive,
            persistenceEncoding.getDataId()) <= 0))
        && ((endDataIdInclusive == null)
            || (UnsignedBytes.lexicographicalComparator().compare(
                endDataIdInclusive,
                persistenceEncoding.getDataId()) >= 0));
  }


  public byte[] getStartDataIdInclusive() {
    return startDataIdInclusive;
  }

  public byte[] getEndDataIdInclusive() {
    return endDataIdInclusive;
  }

  @Override
  public byte[] toBinary() {
    int size = 1;
    byte nullIndicator = 0;
    if (startDataIdInclusive != null) {
      size +=
          (VarintUtils.unsignedIntByteLength(startDataIdInclusive.length)
              + startDataIdInclusive.length);
    } else {
      nullIndicator++;
    }
    if (endDataIdInclusive != null) {
      size +=
          (VarintUtils.unsignedIntByteLength(endDataIdInclusive.length)
              + endDataIdInclusive.length);
    } else {
      nullIndicator += 2;
    }
    final ByteBuffer buf = ByteBuffer.allocate(size);
    buf.put(nullIndicator);
    if (startDataIdInclusive != null) {
      VarintUtils.writeUnsignedInt(startDataIdInclusive.length, buf);
      buf.put(startDataIdInclusive);
    }
    if (endDataIdInclusive != null) {
      VarintUtils.writeUnsignedInt(endDataIdInclusive.length, buf);
      buf.put(endDataIdInclusive);
    }
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final byte nullIndicator = buf.get();
    if ((nullIndicator % 2) == 0) {
      startDataIdInclusive = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
    } else {
      startDataIdInclusive = null;
    }
    if (nullIndicator < 2) {
      endDataIdInclusive = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
    } else {
      endDataIdInclusive = null;
    }

  }
}
