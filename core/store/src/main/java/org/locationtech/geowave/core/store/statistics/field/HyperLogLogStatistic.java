/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.field;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

/**
 * Provides an estimated cardinality of the number of unique values for an attribute.
 */
public class HyperLogLogStatistic extends
    FieldStatistic<HyperLogLogStatistic.HyperLogLogPlusValue> {
  private static final Logger LOGGER = LoggerFactory.getLogger(HyperLogLogStatistic.class);
  public static final FieldStatisticType<HyperLogLogPlusValue> STATS_TYPE =
      new FieldStatisticType<>("HYPER_LOG_LOG");

  @Parameter(
      names = "--precision",
      description = "Number of bits per count value. 2^precision will be the maximum count per distinct value. Maximum precision is 32.",
      validateValueWith = PrecisionValidator.class)
  private int precision = 16;


  public HyperLogLogStatistic() {
    super(STATS_TYPE);
  }

  public HyperLogLogStatistic(final String typeName, final String fieldName) {
    super(STATS_TYPE, typeName, fieldName);
  }

  public HyperLogLogStatistic(final String typeName, final String fieldName, final int precision) {
    super(STATS_TYPE, typeName, fieldName);
    this.precision = precision;
  }

  public void setPrecision(final short precision) {
    this.precision = precision;
  }

  public int getPrecision() {
    return precision;
  }

  @Override
  public String getDescription() {
    return "Provides an estimated cardinality of the number of unqiue values for an attribute.";
  }

  @Override
  public HyperLogLogPlusValue createEmpty() {
    return new HyperLogLogPlusValue(this);
  }

  @Override
  public boolean isCompatibleWith(Class<?> fieldClass) {
    return true;
  }

  @Override
  protected int byteLength() {
    return super.byteLength() + VarintUtils.unsignedIntByteLength(precision);
  }

  @Override
  protected void writeBytes(ByteBuffer buffer) {
    super.writeBytes(buffer);
    VarintUtils.writeUnsignedInt(precision, buffer);
  }

  @Override
  protected void readBytes(ByteBuffer buffer) {
    super.readBytes(buffer);
    precision = VarintUtils.readUnsignedInt(buffer);
  }

  public static class HyperLogLogPlusValue extends StatisticValue<HyperLogLogPlus> implements
      StatisticsIngestCallback {
    private HyperLogLogPlus loglog;

    public HyperLogLogPlusValue() {
      super(null);
      loglog = null;
    }

    public HyperLogLogPlusValue(final HyperLogLogStatistic statistic) {
      super(statistic);
      loglog = new HyperLogLogPlus(statistic.precision);
    }

    public long cardinality() {
      return loglog.cardinality();
    }

    @Override
    public void merge(Mergeable merge) {
      if (merge instanceof HyperLogLogPlusValue) {
        try {
          loglog = (HyperLogLogPlus) ((HyperLogLogPlusValue) merge).loglog.merge(loglog);
        } catch (final CardinalityMergeException e) {
          throw new RuntimeException("Unable to merge counters", e);
        }
      }
    }

    @Override
    public <T> void entryIngested(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      final Object o =
          adapter.getFieldValue(entry, ((HyperLogLogStatistic) statistic).getFieldName());
      if (o == null) {
        return;
      }
      loglog.offer(o.toString());
    }

    @Override
    public HyperLogLogPlus getValue() {
      return loglog;
    }

    @Override
    public byte[] toBinary() {
      try {
        return loglog.getBytes();
      } catch (final IOException e) {
        LOGGER.error("Exception while writing statistic", e);
      }
      return new byte[0];
    }

    @Override
    public void fromBinary(byte[] bytes) {
      try {
        loglog = HyperLogLogPlus.Builder.build(bytes);
      } catch (final IOException e) {
        LOGGER.error("Exception while reading statistic", e);
      }
    }
  }

  private static class PrecisionValidator implements IValueValidator<Integer> {

    @Override
    public void validate(String name, Integer value) throws ParameterException {
      if (value < 1 || value > 32) {
        throw new ParameterException("Precision must be a value between 1 and 32.");
      }
    }

  }

  @Override
  public String toString() {
    final StringBuffer buffer = new StringBuffer();
    buffer.append("HYPER_LOG_LOG[type=").append(getTypeName());
    buffer.append(", field=").append(getFieldName());
    buffer.append(", precision=").append(precision);
    buffer.append("]");
    return buffer.toString();
  }
}
