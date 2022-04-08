/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.field;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;
import com.beust.jcommander.Parameter;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.clearspring.analytics.stream.frequency.FrequencyMergeException;

/**
 * Maintains an estimate of how may of each attribute value occurs in a set of data.
 *
 * <p> Default values:
 *
 * <p> Error factor of 0.001 with probability 0.98 of retrieving a correct estimate. The Algorithm
 * does not under-state the estimate.
 */
public class CountMinSketchStatistic extends
    FieldStatistic<CountMinSketchStatistic.CountMinSketchValue> {
  public static final FieldStatisticType<CountMinSketchValue> STATS_TYPE =
      new FieldStatisticType<>("COUNT_MIN_SKETCH");

  @Parameter(names = "--errorFactor", description = "Error factor.")
  private double errorFactor = 0.001;

  @Parameter(
      names = "--probabilityOfCorrectness",
      description = "Probability of retrieving a correct estimate.")
  private double probabilityOfCorrectness = 0.98;

  public CountMinSketchStatistic() {
    super(STATS_TYPE);
  }

  public CountMinSketchStatistic(final String typeName, final String fieldName) {
    super(STATS_TYPE, typeName, fieldName);
  }

  public CountMinSketchStatistic(
      final String typeName,
      final String fieldName,
      final double errorFactor,
      final double probabilityOfCorrectness) {
    super(STATS_TYPE, typeName, fieldName);
    this.errorFactor = errorFactor;
    this.probabilityOfCorrectness = probabilityOfCorrectness;
  }

  public void setErrorFactor(final double errorFactor) {
    this.errorFactor = errorFactor;
  }

  public double getErrorFactor() {
    return this.errorFactor;
  }

  public void setProbabilityOfCorrectness(final double probabilityOfCorrectness) {
    this.probabilityOfCorrectness = probabilityOfCorrectness;
  }

  public double getProbabilityOfCorrectness() {
    return this.probabilityOfCorrectness;
  }

  @Override
  public String getDescription() {
    return "Maintains an estimate of how many of each attribute value occurs in a set of data.";
  }

  @Override
  public boolean isCompatibleWith(Class<?> fieldClass) {
    return true;
  }

  @Override
  public CountMinSketchValue createEmpty() {
    return new CountMinSketchValue(this);
  }

  @Override
  protected int byteLength() {
    return super.byteLength() + Double.BYTES * 2;
  }

  @Override
  protected void writeBytes(ByteBuffer buffer) {
    super.writeBytes(buffer);
    buffer.putDouble(errorFactor);
    buffer.putDouble(probabilityOfCorrectness);
  }

  @Override
  protected void readBytes(ByteBuffer buffer) {
    super.readBytes(buffer);
    errorFactor = buffer.getDouble();
    probabilityOfCorrectness = buffer.getDouble();
  }

  public static class CountMinSketchValue extends StatisticValue<CountMinSketch> implements
      StatisticsIngestCallback {

    private CountMinSketch sketch;

    public CountMinSketchValue() {
      super(null);
      sketch = null;
    }

    public CountMinSketchValue(final CountMinSketchStatistic statistic) {
      super(statistic);
      sketch =
          new CountMinSketch(statistic.errorFactor, statistic.probabilityOfCorrectness, 7364181);
    }

    public long totalSampleSize() {
      return sketch.size();
    }

    public long count(final String item) {
      return sketch.estimateCount(item);
    }

    @Override
    public void merge(Mergeable merge) {
      if (merge instanceof CountMinSketchValue) {
        try {
          sketch = CountMinSketch.merge(sketch, ((CountMinSketchValue) merge).sketch);
        } catch (final FrequencyMergeException e) {
          throw new RuntimeException("Unable to merge sketches", e);
        }
      }
    }

    @Override
    public <T> void entryIngested(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      final Object o =
          adapter.getFieldValue(entry, ((CountMinSketchStatistic) statistic).getFieldName());
      if (o == null) {
        return;
      }
      sketch.add(o.toString(), 1);
    }

    @Override
    public CountMinSketch getValue() {
      return sketch;
    }

    @Override
    public byte[] toBinary() {
      final byte[] data = CountMinSketch.serialize(sketch);
      final ByteBuffer buffer =
          ByteBuffer.allocate(VarintUtils.unsignedIntByteLength(data.length) + data.length);
      VarintUtils.writeUnsignedInt(data.length, buffer);
      buffer.put(data);
      return buffer.array();
    }

    @Override
    public void fromBinary(byte[] bytes) {
      final ByteBuffer buffer = ByteBuffer.wrap(bytes);
      final byte[] data = ByteArrayUtils.safeRead(buffer, VarintUtils.readUnsignedInt(buffer));
      sketch = CountMinSketch.deserialize(data);
    }
  }
}
