/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * Base aggregation class for performing math operations on numeric attributes. It uses BigDecimal
 * due to it being the most precise numeric attribute possible.
 */
public abstract class FieldMathAggregation<T> implements
    Aggregation<FieldNameParam, BigDecimal, T> {
  private FieldNameParam fieldNameParam;
  private BigDecimal value = null;

  public FieldMathAggregation() {
    this(null);
  }

  public FieldMathAggregation(final FieldNameParam fieldNameParam) {
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
  public BigDecimal getResult() {
    return value;
  }

  @Override
  public BigDecimal merge(final BigDecimal result1, final BigDecimal result2) {
    return agg(result1, result2);
  }

  @Override
  public byte[] resultToBinary(BigDecimal result) {
    return VarintUtils.writeBigDecimal(result);
  }

  @Override
  public BigDecimal resultFromBinary(byte[] binary) {
    return VarintUtils.readBigDecimal(ByteBuffer.wrap(binary));
  }

  @Override
  public void clearResult() {
    value = null;
  }

  @Override
  public void aggregate(final DataTypeAdapter<T> adapter, T entry) {
    Object o;
    if ((fieldNameParam != null) && !fieldNameParam.isEmpty()) {
      o = adapter.getFieldValue(entry, fieldNameParam.getFieldName());
      if (o instanceof Number) {
        value = agg(value, new BigDecimal(o.toString()));
      }
    }
  }

  protected abstract BigDecimal agg(final BigDecimal a, final BigDecimal b);

}
