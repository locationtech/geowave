/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.aggregation;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Counts non-null values of a simple feature attribute. If no attribute is specified, counts each
 * simple feature.
 */
public class VectorCountAggregation implements Aggregation<FieldNameParam, Long, SimpleFeature> {
  private FieldNameParam fieldNameParam;
  private long count = 0;

  public VectorCountAggregation() {
    this(null);
  }

  public VectorCountAggregation(final FieldNameParam fieldNameParam) {
    super();
    this.fieldNameParam = fieldNameParam;
  }

  @Override
  public FieldNameParam getParameters() {
    return fieldNameParam;
  }

  @Override
  public void setParameters(final FieldNameParam fieldNameParam) {
    this.fieldNameParam = fieldNameParam;
  }

  @Override
  public Long merge(final Long result1, final Long result2) {
    return result1 + result2;
  }

  @Override
  public Long getResult() {
    return count;
  }

  @Override
  public byte[] resultToBinary(final Long result) {
    final ByteBuffer buffer = ByteBuffer.allocate(VarintUtils.unsignedLongByteLength(result));
    VarintUtils.writeUnsignedLong(result, buffer);
    return buffer.array();
  }

  @Override
  public Long resultFromBinary(final byte[] binary) {
    return VarintUtils.readUnsignedLong(ByteBuffer.wrap(binary));
  }

  @Override
  public void clearResult() {
    count = 0;
  }

  @Override
  public void aggregate(final DataTypeAdapter<SimpleFeature> adapter, final SimpleFeature entry) {
    Object o;
    if ((fieldNameParam != null) && !fieldNameParam.isEmpty()) {
      o = entry.getAttribute(fieldNameParam.getFieldName());
      if (o != null) {
        count++;
      }
    } else {
      count++;
    }
  }

}
