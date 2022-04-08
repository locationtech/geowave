/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.dimension;

import java.nio.ByteBuffer;
import java.util.Set;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.geotime.store.field.IntervalSerializationProvider;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.dimension.bin.BinRange;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import org.locationtech.geowave.core.index.numeric.NumericValue;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.threeten.extra.Interval;
import com.google.common.collect.Sets;

/**
 * This field definition can be used for temporal data (either as a time range or a single instant
 * in time).
 */
public class TimeField implements NumericDimensionField<Interval> {
  public static final String DEFAULT_FIELD_ID = "default_time_dimension";
  public static final IndexDimensionHint TIME_DIMENSION_HINT = new IndexDimensionHint("TIME");
  public static final IndexDimensionHint START_TIME_DIMENSION_HINT =
      new IndexDimensionHint("START_TIME");
  public static final IndexDimensionHint END_TIME_DIMENSION_HINT =
      new IndexDimensionHint("END_TIME");
  private NumericDimensionDefinition baseDefinition;
  private final FieldReader<Interval> reader;
  private final FieldWriter<Interval> writer;
  private String fieldName;

  public TimeField() {
    final IntervalSerializationProvider serializationProvider = new IntervalSerializationProvider();
    reader = serializationProvider.getFieldReader();
    writer = serializationProvider.getFieldWriter();
    fieldName = DEFAULT_FIELD_ID;
  }

  public TimeField(final Unit timeUnit) {
    this(timeUnit, DEFAULT_FIELD_ID);
  }

  public TimeField(final Unit timeUnit, final String fieldName) {
    this(new TimeDefinition(timeUnit), fieldName);
  }

  @Override
  public NumericData getFullRange() {
    return new NumericRange(0, System.currentTimeMillis() + 1);
  }

  public TimeField(final NumericDimensionDefinition baseDefinition, final String fieldName) {
    this.baseDefinition = baseDefinition;
    final IntervalSerializationProvider serializationProvider = new IntervalSerializationProvider();
    reader = serializationProvider.getFieldReader();
    writer = serializationProvider.getFieldWriter();
    this.fieldName = fieldName;
  }

  @Override
  public double normalize(final double value) {
    return baseDefinition.normalize(value);
  }

  @Override
  public double denormalize(final double value) {
    return baseDefinition.denormalize(value);
  }

  @Override
  public BinRange[] getNormalizedRanges(final NumericData index) {
    return baseDefinition.getNormalizedRanges(index);
  }

  @Override
  public NumericRange getDenormalizedRange(final BinRange range) {
    return baseDefinition.getDenormalizedRange(range);
  }

  @Override
  public int getFixedBinIdSize() {
    return baseDefinition.getFixedBinIdSize();
  }

  @Override
  public double getRange() {
    return baseDefinition.getRange();
  }

  @Override
  public NumericRange getBounds() {
    return baseDefinition.getBounds();
  }

  @Override
  public NumericData getNumericData(final Interval dataElement) {
    if (dataElement.getStart().equals(dataElement.getEnd())) {
      return new NumericValue(dataElement.getStart().toEpochMilli());
    }
    return new NumericRange(
        dataElement.getStart().toEpochMilli(),
        dataElement.getEnd().toEpochMilli());
  }

  @Override
  public String getFieldName() {
    return fieldName;
  }

  @Override
  public FieldWriter<Interval> getWriter() {
    return writer;
  }

  @Override
  public FieldReader<Interval> getReader() {
    return reader;
  }

  @Override
  public NumericDimensionDefinition getBaseDefinition() {
    return baseDefinition;
  }

  @Override
  public byte[] toBinary() {
    final byte[] dimensionBinary = PersistenceUtils.toBinary(baseDefinition);
    final byte[] fieldNameBytes = StringUtils.stringToBinary(fieldName);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            dimensionBinary.length
                + fieldNameBytes.length
                + VarintUtils.unsignedIntByteLength(fieldNameBytes.length));
    VarintUtils.writeUnsignedInt(fieldNameBytes.length, buf);
    buf.put(fieldNameBytes);
    buf.put(dimensionBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int fieldNameLength = VarintUtils.readUnsignedInt(buf);
    final byte[] fieldNameBinary = ByteArrayUtils.safeRead(buf, fieldNameLength);
    fieldName = StringUtils.stringFromBinary(fieldNameBinary);

    final byte[] dimensionBinary = new byte[buf.remaining()];
    buf.get(dimensionBinary);
    baseDefinition = (NumericDimensionDefinition) PersistenceUtils.fromBinary(dimensionBinary);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((baseDefinition == null) ? 0 : baseDefinition.hashCode());
    result = (prime * result) + ((fieldName == null) ? 0 : fieldName.hashCode());
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
    final TimeField other = (TimeField) obj;
    if (baseDefinition == null) {
      if (other.baseDefinition != null) {
        return false;
      }
    } else if (!baseDefinition.equals(other.baseDefinition)) {
      return false;
    }
    if (fieldName == null) {
      if (other.fieldName != null) {
        return false;
      }
    } else if (!fieldName.equals(other.fieldName)) {
      return false;
    }
    return true;
  }

  @Override
  public Class<Interval> getFieldClass() {
    return Interval.class;
  }

  @Override
  public Set<IndexDimensionHint> getDimensionHints() {
    return Sets.newHashSet(TIME_DIMENSION_HINT, START_TIME_DIMENSION_HINT, END_TIME_DIMENSION_HINT);
  }
}
