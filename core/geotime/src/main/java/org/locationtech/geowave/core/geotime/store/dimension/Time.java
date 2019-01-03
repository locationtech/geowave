/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.dimension;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.index.sfc.data.NumericValue;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexValue;

/**
 * The base interface for time values, could be either a time range (an interval) or a timestamp (an
 * instant)
 */
public interface Time extends Persistable, CommonIndexValue {
  /**
   * A range of time. This class wraps a start and stop instant in milliseconds with a visibility
   * tag for the field value.
   */
  public static class TimeRange implements Time {
    private long startTime;
    private long endTime;
    private byte[] visibility;

    public TimeRange() {}

    public TimeRange(final long startTime, final long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
    }

    public TimeRange(final long startTime, final long endTime, final byte[] visibility) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.visibility = visibility;
    }

    @Override
    public byte[] getVisibility() {
      return visibility;
    }

    @Override
    public void setVisibility(final byte[] visibility) {
      this.visibility = visibility;
    }

    @Override
    public NumericData toNumericData() {
      return new NumericRange(startTime, endTime);
    }

    @Override
    public byte[] toBinary() {
      final ByteBuffer bytes =
          ByteBuffer.allocate(
              VarintUtils.timeByteLength(startTime) + VarintUtils.timeByteLength(endTime));
      VarintUtils.writeTime(startTime, bytes);
      VarintUtils.writeTime(endTime, bytes);
      return bytes.array();
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      startTime = VarintUtils.readTime(buf);
      endTime = VarintUtils.readTime(buf);
    }

    @Override
    public boolean overlaps(final NumericDimensionField[] field, final NumericData[] rangeData) {
      assert (field[0] instanceof TimeField);
      long t0 = (long) Math.ceil(rangeData[0].getMax()) - this.startTime;
      long t1 = this.endTime - (long) Math.floor(rangeData[0].getMin());
      return Math.abs(t0 - t1) <= (t0 + t1);
    }
  }

  /** An instant of time in milliseconds wrapped with a visibility tag for the field value. */
  public static class Timestamp implements Time {
    private long time;
    private byte[] visibility;

    public Timestamp() {}

    public Timestamp(final long time) {
      this.time = time;
    }

    public Timestamp(final long time, final byte[] visibility) {
      this.time = time;
      this.visibility = visibility;
    }

    @Override
    public byte[] getVisibility() {
      return visibility;
    }

    @Override
    public void setVisibility(final byte[] visibility) {
      this.visibility = visibility;
    }

    @Override
    public NumericData toNumericData() {
      return new NumericValue(time);
    }

    @Override
    public byte[] toBinary() {
      final ByteBuffer bytes = ByteBuffer.allocate(VarintUtils.timeByteLength(time));
      VarintUtils.writeTime(time, bytes);
      return bytes.array();
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      time = VarintUtils.readTime(buf);
    }

    @Override
    public boolean overlaps(final NumericDimensionField[] field, final NumericData[] rangeData) {
      assert (field[0] instanceof TimeField);
      return (long) Math.floor(rangeData[0].getMin()) <= this.time
          && (long) Math.ceil(rangeData[0].getMax()) >= this.time;
    }
  }

  public NumericData toNumericData();
}
