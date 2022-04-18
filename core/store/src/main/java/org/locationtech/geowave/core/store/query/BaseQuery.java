/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;

public abstract class BaseQuery<T, O extends DataTypeQueryOptions<T>> implements Persistable {
  private CommonQueryOptions commonQueryOptions;
  private O dataTypeQueryOptions;
  private IndexQueryOptions indexQueryOptions;
  private QueryConstraints queryConstraints;

  protected BaseQuery() {}

  public BaseQuery(
      final CommonQueryOptions commonQueryOptions,
      final O dataTypeQueryOptions,
      final IndexQueryOptions indexQueryOptions,
      final QueryConstraints queryConstraints) {
    this.commonQueryOptions = commonQueryOptions;
    this.dataTypeQueryOptions = dataTypeQueryOptions;
    this.indexQueryOptions = indexQueryOptions;
    this.queryConstraints = queryConstraints;
  }

  public CommonQueryOptions getCommonQueryOptions() {
    return commonQueryOptions;
  }

  public O getDataTypeQueryOptions() {
    return dataTypeQueryOptions;
  }

  public IndexQueryOptions getIndexQueryOptions() {
    return indexQueryOptions;
  }

  public QueryConstraints getQueryConstraints() {
    return queryConstraints;
  }

  @Override
  public byte[] toBinary() {
    byte[] commonQueryOptionsBinary, dataTypeQueryOptionsBinary, indexQueryOptionsBinary,
        queryConstraintsBinary;
    if (commonQueryOptions != null) {
      commonQueryOptionsBinary = PersistenceUtils.toBinary(commonQueryOptions);
    } else {
      commonQueryOptionsBinary = new byte[0];
    }
    if (dataTypeQueryOptions != null) {
      dataTypeQueryOptionsBinary = PersistenceUtils.toBinary(dataTypeQueryOptions);
    } else {
      dataTypeQueryOptionsBinary = new byte[0];
    }
    if (indexQueryOptions != null) {
      indexQueryOptionsBinary = PersistenceUtils.toBinary(indexQueryOptions);
    } else {
      indexQueryOptionsBinary = new byte[0];
    }
    if (queryConstraints != null) {
      queryConstraintsBinary = PersistenceUtils.toBinary(queryConstraints);
    } else {
      queryConstraintsBinary = new byte[0];
    }
    final ByteBuffer buf =
        ByteBuffer.allocate(
            commonQueryOptionsBinary.length
                + dataTypeQueryOptionsBinary.length
                + indexQueryOptionsBinary.length
                + queryConstraintsBinary.length
                + VarintUtils.unsignedIntByteLength(commonQueryOptionsBinary.length)
                + VarintUtils.unsignedIntByteLength(dataTypeQueryOptionsBinary.length)
                + VarintUtils.unsignedIntByteLength(indexQueryOptionsBinary.length));
    VarintUtils.writeUnsignedInt(commonQueryOptionsBinary.length, buf);
    buf.put(commonQueryOptionsBinary);
    VarintUtils.writeUnsignedInt(dataTypeQueryOptionsBinary.length, buf);
    buf.put(dataTypeQueryOptionsBinary);
    VarintUtils.writeUnsignedInt(indexQueryOptionsBinary.length, buf);
    buf.put(indexQueryOptionsBinary);
    buf.put(queryConstraintsBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int commonQueryOptionsBinaryLength = VarintUtils.readUnsignedInt(buf);
    if (commonQueryOptionsBinaryLength == 0) {
      commonQueryOptions = null;
    } else {
      final byte[] commonQueryOptionsBinary =
          ByteArrayUtils.safeRead(buf, commonQueryOptionsBinaryLength);
      commonQueryOptions =
          (CommonQueryOptions) PersistenceUtils.fromBinary(commonQueryOptionsBinary);
    }
    final int dataTypeQueryOptionsBinaryLength = VarintUtils.readUnsignedInt(buf);
    if (dataTypeQueryOptionsBinaryLength == 0) {
      dataTypeQueryOptions = null;
    } else {
      final byte[] dataTypeQueryOptionsBinary =
          ByteArrayUtils.safeRead(buf, dataTypeQueryOptionsBinaryLength);
      dataTypeQueryOptions = (O) PersistenceUtils.fromBinary(dataTypeQueryOptionsBinary);
    }
    final int indexQueryOptionsBinaryLength = VarintUtils.readUnsignedInt(buf);
    if (indexQueryOptionsBinaryLength == 0) {
      indexQueryOptions = null;
    } else {
      final byte[] indexQueryOptionsBinary =
          ByteArrayUtils.safeRead(buf, indexQueryOptionsBinaryLength);
      indexQueryOptions = (IndexQueryOptions) PersistenceUtils.fromBinary(indexQueryOptionsBinary);
    }
    final byte[] queryConstraintsBinary = new byte[buf.remaining()];
    if (queryConstraintsBinary.length == 0) {
      queryConstraints = null;
    } else {
      buf.get(queryConstraintsBinary);
      queryConstraints = (QueryConstraints) PersistenceUtils.fromBinary(queryConstraintsBinary);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((commonQueryOptions == null) ? 0 : commonQueryOptions.hashCode());
    result =
        (prime * result) + ((dataTypeQueryOptions == null) ? 0 : dataTypeQueryOptions.hashCode());
    result = (prime * result) + ((indexQueryOptions == null) ? 0 : indexQueryOptions.hashCode());
    result = (prime * result) + ((queryConstraints == null) ? 0 : queryConstraints.hashCode());
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
    final BaseQuery other = (BaseQuery) obj;
    if (commonQueryOptions == null) {
      if (other.commonQueryOptions != null) {
        return false;
      }
    } else if (!commonQueryOptions.equals(other.commonQueryOptions)) {
      return false;
    }
    if (dataTypeQueryOptions == null) {
      if (other.dataTypeQueryOptions != null) {
        return false;
      }
    } else if (!dataTypeQueryOptions.equals(other.dataTypeQueryOptions)) {
      return false;
    }
    if (indexQueryOptions == null) {
      if (other.indexQueryOptions != null) {
        return false;
      }
    } else if (!indexQueryOptions.equals(other.indexQueryOptions)) {
      return false;
    }
    if (queryConstraints == null) {
      if (other.queryConstraints != null) {
        return false;
      }
    } else if (!queryConstraints.equals(other.queryConstraints)) {
      return false;
    }
    return true;
  }
}
