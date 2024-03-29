/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.migration.legacy.core.store;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.IndexStore;

public class LegacyAdapterToIndexMapping implements Persistable {

  private short adapterId;
  private String[] indexNames;

  public LegacyAdapterToIndexMapping() {}

  public LegacyAdapterToIndexMapping(final short adapterId, final Index[] indices) {
    super();
    this.adapterId = adapterId;
    indexNames = new String[indices.length];
    for (int i = 0; i < indices.length; i++) {
      indexNames[i] = indices[i].getName();
    }
  }

  public LegacyAdapterToIndexMapping(final short adapterId, final String... indexNames) {
    super();
    this.adapterId = adapterId;
    this.indexNames = indexNames;
  }

  public short getAdapterId() {
    return adapterId;
  }

  public String[] getIndexNames() {
    return indexNames;
  }

  public Index[] getIndices(final IndexStore indexStore) {
    final Index[] indices = new Index[indexNames.length];
    for (int i = 0; i < indexNames.length; i++) {
      indices[i] = indexStore.getIndex(indexNames[i]);
    }
    return indices;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((adapterId == 0) ? 0 : Short.hashCode(adapterId));
    result = (prime * result) + Arrays.hashCode(indexNames);
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
    final LegacyAdapterToIndexMapping other = (LegacyAdapterToIndexMapping) obj;
    if (adapterId == 0) {
      if (other.adapterId != 0) {
        return false;
      }
    } else if (adapterId != other.adapterId) {
      return false;
    }
    if (!Arrays.equals(indexNames, other.indexNames)) {
      return false;
    }
    return true;
  }

  public boolean contains(final String indexName) {
    for (final String id : indexNames) {
      if (id.equals(indexName)) {
        return true;
      }
    }
    return false;
  }

  public boolean isNotEmpty() {
    return indexNames.length > 0;
  }

  @Override
  public byte[] toBinary() {
    final byte[] indexIdBytes = StringUtils.stringsToBinary(indexNames);
    final ByteBuffer buf =
        ByteBuffer.allocate(VarintUtils.unsignedShortByteLength(adapterId) + indexIdBytes.length);
    VarintUtils.writeUnsignedShort(adapterId, buf);
    buf.put(indexIdBytes);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    adapterId = VarintUtils.readUnsignedShort(buf);
    final byte[] indexNamesBytes = new byte[buf.remaining()];
    buf.get(indexNamesBytes);
    indexNames = StringUtils.stringsFromBinary(indexNamesBytes);
  }

}
