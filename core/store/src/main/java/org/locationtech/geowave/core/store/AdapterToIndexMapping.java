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
import java.util.List;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;
import org.locationtech.geowave.core.store.index.IndexStore;

/** Meta-data for retaining Adapter to Index association */
public class AdapterToIndexMapping implements Persistable {
  private short adapterId;
  private String indexName;
  private List<IndexFieldMapper<?, ?>> fieldMappers;

  public AdapterToIndexMapping() {}

  public AdapterToIndexMapping(
      final short adapterId,
      final Index index,
      final List<IndexFieldMapper<?, ?>> fieldMappers) {
    super();
    this.adapterId = adapterId;
    indexName = index.getName();
    this.fieldMappers = fieldMappers;
  }

  public AdapterToIndexMapping(
      final short adapterId,
      final String indexName,
      final List<IndexFieldMapper<?, ?>> fieldMappers) {
    super();
    this.adapterId = adapterId;
    this.indexName = indexName;
    this.fieldMappers = fieldMappers;
  }

  public short getAdapterId() {
    return adapterId;
  }

  public String getIndexName() {
    return indexName;
  }

  public List<IndexFieldMapper<?, ?>> getIndexFieldMappers() {
    return fieldMappers;
  }

  public IndexFieldMapper<?, ?> getMapperForIndexField(final String indexFieldName) {
    return fieldMappers.stream().filter(
        mapper -> mapper.indexFieldName().equals(indexFieldName)).findFirst().orElse(null);
  }

  public Index getIndex(final IndexStore indexStore) {
    return indexStore.getIndex(indexName);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((adapterId == 0) ? 0 : Short.hashCode(adapterId));
    result = (prime * result) + indexName.hashCode();
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
    final AdapterToIndexMapping other = (AdapterToIndexMapping) obj;
    if (adapterId == 0) {
      if (other.adapterId != 0) {
        return false;
      }
    } else if (adapterId != other.adapterId) {
      return false;
    }
    if (!indexName.equals(other.indexName)) {
      return false;
    }
    return true;
  }

  @Override
  public byte[] toBinary() {
    final byte[] indexIdBytes = StringUtils.stringToBinary(indexName);
    final byte[] mapperBytes = PersistenceUtils.toBinary(fieldMappers);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            VarintUtils.unsignedShortByteLength(adapterId)
                + VarintUtils.unsignedShortByteLength((short) indexIdBytes.length)
                + indexIdBytes.length
                + mapperBytes.length);
    VarintUtils.writeUnsignedShort(adapterId, buf);
    VarintUtils.writeUnsignedShort((short) indexIdBytes.length, buf);
    buf.put(indexIdBytes);
    buf.put(mapperBytes);
    return buf.array();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    adapterId = VarintUtils.readUnsignedShort(buf);
    final byte[] indexNameBytes = new byte[VarintUtils.readUnsignedShort(buf)];
    buf.get(indexNameBytes);
    indexName = StringUtils.stringFromBinary(indexNameBytes);
    final byte[] mapperBytes = new byte[buf.remaining()];
    buf.get(mapperBytes);
    fieldMappers = (List) PersistenceUtils.fromBinaryAsList(mapperBytes);
  }
}
