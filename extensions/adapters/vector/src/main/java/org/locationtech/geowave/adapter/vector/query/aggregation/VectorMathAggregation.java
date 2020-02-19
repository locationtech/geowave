/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.aggregation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Base aggregation class for performing math operations on numeric simple feature attributes. It
 * uses BigDecimal due to it being the most precise numeric attribute possible.
 */
public abstract class VectorMathAggregation implements
    Aggregation<FieldNameParam, BigDecimal, SimpleFeature> {
  private FieldNameParam fieldNameParam;
  private BigDecimal value = null;

  public VectorMathAggregation() {
    this(null);
  }

  public VectorMathAggregation(final FieldNameParam fieldNameParam) {
    super();
    this.fieldNameParam = fieldNameParam;
  }

  @Override
  public byte[] toBinary() {
    return new byte[0];
  }

  @Override
  public void fromBinary(final byte[] bytes) {}

  @Override
  public FieldNameParam getParameters() {
    return fieldNameParam;
  }

  @Override
  public void setParameters(final FieldNameParam fieldNameParam) {
    this.fieldNameParam = fieldNameParam;
  }

  @Override
  public BigDecimal merge(final BigDecimal result1, final BigDecimal result2) {
    return agg(result1, result2);
  }

  @Override
  public BigDecimal getResult() {
    return value;
  }

  @Override
  public byte[] resultToBinary(BigDecimal result) {
    if (result == null) {
      return new byte[0];
    }
    final byte[] unscaled = result.unscaledValue().toByteArray();
    final ByteBuffer buf =
        ByteBuffer.allocate(VarintUtils.signedIntByteLength(result.scale()) + unscaled.length);
    VarintUtils.writeSignedInt(result.scale(), buf);
    buf.put(unscaled);
    return buf.array();
  }

  @Override
  public BigDecimal resultFromBinary(byte[] binary) {
    if (binary.length == 0) {
      return null;
    }
    final ByteBuffer bb = ByteBuffer.wrap(binary);
    final int scale = VarintUtils.readSignedInt(bb);
    final byte[] unscaled = new byte[bb.remaining()];
    bb.get(unscaled);
    return new BigDecimal(new BigInteger(unscaled), scale);
  }

  @Override
  public void clearResult() {
    value = null;
  }

  @Override
  public void aggregate(SimpleFeature entry) {
    Object o;
    if ((fieldNameParam != null) && !fieldNameParam.isEmpty()) {
      o = entry.getAttribute(fieldNameParam.getFieldName());
      if (o != null && Number.class.isAssignableFrom(o.getClass())) {
        value = agg(value, new BigDecimal(((Number) o).toString()));
      }
    }
  }

  protected abstract BigDecimal agg(final BigDecimal a, final BigDecimal b);

}
