/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;

public class OptimalCountAggregation<T> extends OptimalFieldAggregation<Long, T> {

  public OptimalCountAggregation() {
    super();
  }

  public OptimalCountAggregation(final FieldNameParam fieldNameParam) {
    super(fieldNameParam);
  }

  @Override
  protected Aggregation<FieldNameParam, Long, CommonIndexedPersistenceEncoding> createCommonIndexAggregation() {
    return new CommonIndexCountAggregation(fieldNameParam);
  }

  @Override
  protected Aggregation<FieldNameParam, Long, T> createAggregation() {
    return new FieldCountAggregation<>(fieldNameParam);
  }

  public static class CommonIndexCountAggregation implements
      CommonIndexAggregation<FieldNameParam, Long> {
    private FieldNameParam fieldNameParam;
    private long count = 0;

    public CommonIndexCountAggregation() {
      fieldNameParam = null;
    }

    public CommonIndexCountAggregation(final FieldNameParam param) {
      this.fieldNameParam = param;
    }

    @Override
    public FieldNameParam getParameters() {
      return fieldNameParam;
    }

    @Override
    public void setParameters(FieldNameParam parameters) {
      this.fieldNameParam = parameters;
    }

    @Override
    public Long getResult() {
      return count;
    }

    @Override
    public byte[] resultToBinary(Long result) {
      final ByteBuffer buffer = ByteBuffer.allocate(VarintUtils.unsignedLongByteLength(result));
      VarintUtils.writeUnsignedLong(result, buffer);
      return buffer.array();
    }

    @Override
    public Long resultFromBinary(byte[] binary) {
      return VarintUtils.readUnsignedLong(ByteBuffer.wrap(binary));
    }

    @Override
    public Long merge(final Long value1, final Long value2) {
      return value1 + value2;
    }

    @Override
    public void clearResult() {
      count = 0;
    }

    @Override
    public void aggregate(
        DataTypeAdapter<CommonIndexedPersistenceEncoding> adapter,
        CommonIndexedPersistenceEncoding entry) {
      if (fieldNameParam == null) {
        count++;
      } else if (entry.getCommonData().getValue(fieldNameParam.getFieldName()) != null) {
        count++;
      }
    }
  }

  public static class FieldCountAggregation<T> implements Aggregation<FieldNameParam, Long, T> {
    private FieldNameParam fieldNameParam;
    private long count = 0;

    public FieldCountAggregation() {
      fieldNameParam = null;
    }

    public FieldCountAggregation(final FieldNameParam fieldNameParam) {
      this.fieldNameParam = fieldNameParam;
    }

    @Override
    public FieldNameParam getParameters() {
      return fieldNameParam;
    }

    @Override
    public void setParameters(FieldNameParam parameters) {
      this.fieldNameParam = parameters;
    }

    @Override
    public Long getResult() {
      return count;
    }

    @Override
    public byte[] resultToBinary(Long result) {
      final ByteBuffer buffer = ByteBuffer.allocate(VarintUtils.unsignedLongByteLength(result));
      VarintUtils.writeUnsignedLong(result, buffer);
      return buffer.array();
    }

    @Override
    public Long resultFromBinary(byte[] binary) {
      return VarintUtils.readUnsignedLong(ByteBuffer.wrap(binary));
    }

    @Override
    public Long merge(final Long value1, final Long value2) {
      return value1 + value2;
    }

    @Override
    public void clearResult() {
      count = 0;
    }

    @Override
    public void aggregate(DataTypeAdapter<T> adapter, T entry) {
      if (fieldNameParam == null) {
        count++;
      } else if (adapter.getFieldValue(entry, fieldNameParam.getFieldName()) != null) {
        count++;
      }
    }
  }
}
