/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;

public class AdapterMapping implements Persistable {
  private ByteArray adapterId;
  private short internalAdapterId;

  public AdapterMapping() {}

  public AdapterMapping(final ByteArray adapterId, final short internalAdapterId) {
    super();
    this.adapterId = adapterId;
    this.internalAdapterId = internalAdapterId;
  }

  public ByteArray getAdapterId() {
    return adapterId;
  }

  public short getInteranalAdapterId() {
    return internalAdapterId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((adapterId == null) ? 0 : adapterId.hashCode());
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
    final AdapterMapping other = (AdapterMapping) obj;
    if (adapterId == null) {
      if (other.adapterId != null) {
        return false;
      }
    } else if (!adapterId.equals(other.adapterId)) {
      return false;
    }
    if (internalAdapterId != other.internalAdapterId) {
      return false;
    }
    return true;
  }

  @Override
  public byte[] toBinary() {
    final byte[] adapterIdBytes = adapterId.getBytes();
    final ByteBuffer buf =
        ByteBuffer.allocate(
            adapterIdBytes.length + VarintUtils.unsignedShortByteLength(internalAdapterId));
    buf.put(adapterIdBytes);
    VarintUtils.writeUnsignedShort(internalAdapterId, buf);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    internalAdapterId = VarintUtils.readUnsignedShort(buf);
    final byte[] adapterIdBytes = new byte[buf.remaining()];
    buf.get(adapterIdBytes);
    adapterId = new ByteArray(adapterIdBytes);
  }
}
