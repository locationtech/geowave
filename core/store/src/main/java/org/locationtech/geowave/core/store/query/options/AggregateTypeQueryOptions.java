/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.options;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Aggregation;

public class AggregateTypeQueryOptions<P extends Persistable, R, T> implements
    DataTypeQueryOptions<R> {
  private String[] typeNames;
  private Aggregation<P, R, T> aggregation;

  public AggregateTypeQueryOptions() {}

  public AggregateTypeQueryOptions(
      final Aggregation<P, R, T> aggregation,
      final String... typeNames) {
    this.typeNames = typeNames;
    this.aggregation = aggregation;
  }

  @Override
  public String[] getTypeNames() {
    return typeNames;
  }

  public void setTypeNames(String[] typeNames) {
    this.typeNames = typeNames;
  }

  public Aggregation<P, R, T> getAggregation() {
    return aggregation;
  }

  public void setAggregation(Aggregation<P, R, T> aggregation) {
    this.aggregation = aggregation;
  }

  @Override
  public byte[] toBinary() {
    byte[] typeNamesBinary, aggregationBinary;
    if ((typeNames != null) && (typeNames.length > 0)) {
      typeNamesBinary = StringUtils.stringsToBinary(typeNames);
    } else {
      typeNamesBinary = new byte[0];
    }
    if (aggregation != null) {
      aggregationBinary = PersistenceUtils.toBinary(aggregation);
    } else {
      aggregationBinary = new byte[0];
    }
    final ByteBuffer buf =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(typeNamesBinary.length)
                + aggregationBinary.length
                + typeNamesBinary.length);
    VarintUtils.writeUnsignedInt(typeNamesBinary.length, buf);
    buf.put(typeNamesBinary);
    buf.put(aggregationBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int typeNamesBytesLength = VarintUtils.readUnsignedInt(buf);
    if (typeNamesBytesLength == 0) {
      typeNames = new String[0];
    } else {
      final byte[] typeNamesBytes = ByteArrayUtils.safeRead(buf, typeNamesBytesLength);
      typeNames = StringUtils.stringsFromBinary(typeNamesBytes);
    }
    final byte[] aggregationBytes = new byte[buf.remaining()];
    if (aggregationBytes.length == 0) {
      aggregation = null;
    } else {
      buf.get(aggregationBytes);
      aggregation = (Aggregation<P, R, T>) PersistenceUtils.fromBinary(aggregationBytes);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((aggregation == null) ? 0 : aggregation.hashCode());
    result = (prime * result) + Arrays.hashCode(typeNames);
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
    final AggregateTypeQueryOptions other = (AggregateTypeQueryOptions) obj;
    if (aggregation == null) {
      if (other.aggregation != null) {
        return false;
      }
    } else if (!aggregation.equals(other.aggregation)) {
      return false;
    }
    if (!Arrays.equals(typeNames, other.typeNames)) {
      return false;
    }
    return true;
  }
}
