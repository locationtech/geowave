/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.binning;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Range;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl.ExplicitConstraints;
import com.beust.jcommander.Parameter;

/**
 * Statistic binning strategy that bins statistic values by the numeric representation of the value
 * of a given field. By default it will truncate decimal places and will bin by the integer.
 * However, an "offset" and "interval" can be provided to bin numbers at any regular step-sized
 * increment from an origin value. A statistic using this binning strategy can be constrained using
 * numeric ranges (Apache-Commons `Range<? extends Number>` class can be used as a constraint).
 */
public class NumericRangeFieldValueBinningStrategy extends FieldValueBinningStrategy {
  public static final String NAME = "NUMERIC_RANGE";
  @Parameter(names = "--binInterval", description = "The interval between bins.  Defaults to 1.")
  private double interval = 1;

  @Parameter(
      names = "--binOffset",
      description = "Offset the field values by a given amount.  Defaults to 0.")
  private double offset = 0;

  @Override
  public String getStrategyName() {
    return NAME;
  }

  public NumericRangeFieldValueBinningStrategy() {
    super();
  }

  public NumericRangeFieldValueBinningStrategy(final String... fields) {
    super(fields);
  }

  public NumericRangeFieldValueBinningStrategy(final double interval, final String... fields) {
    this(interval, 0.0, fields);
  }

  public NumericRangeFieldValueBinningStrategy(
      final double interval,
      final double offset,
      final String... fields) {
    super(fields);
    this.interval = interval;
    this.offset = offset;
  }


  @Override
  public String getDescription() {
    return "Bin the statistic by the numeric value of a specified field.";
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<?>[] supportedConstraintClasses() {
    return ArrayUtils.addAll(
        super.supportedConstraintClasses(),
        Number.class,
        Range.class,
        Range[].class);
  }

  @SuppressWarnings("unchecked")
  @Override
  public ByteArrayConstraints singleFieldConstraints(final Object constraint) {
    if (constraint instanceof Number) {
      return new ExplicitConstraints(new ByteArray[] {getNumericBin((Number) constraint)});
    } else if (constraint instanceof Range) {
      return new ExplicitConstraints(getNumericBins((Range<? extends Number>) constraint));
    } else if (constraint instanceof Range[]) {
      final Stream<ByteArray[]> stream =
          Arrays.stream((Range[]) constraint).map(this::getNumericBins);
      return new ExplicitConstraints(stream.flatMap(Arrays::stream).toArray(ByteArray[]::new));
    }
    return super.constraints(constraint);
  }

  @Override
  protected ByteArray getSingleBin(final Object value) {
    if ((value == null) || !(value instanceof Number)) {
      return new ByteArray(new byte[] {0x0});
    }
    return getNumericBin((Number) value);
  }

  private ByteArray getNumericBin(final Number value) {
    final long bin = (long) Math.floor(((value.doubleValue() + offset) / interval));
    return getBinId(bin);
  }

  private ByteArray getBinId(final long bin) {
    final ByteBuffer buffer = ByteBuffer.allocate(1 + Long.BYTES);
    buffer.put((byte) 0x1);
    buffer.putLong(Lexicoders.LONG.lexicode(bin));
    return new ByteArray(buffer.array());
  }

  private ByteArray[] getNumericBins(final Range<? extends Number> value) {
    final long minBin = (long) Math.floor(((value.getMinimum().doubleValue() + offset) / interval));
    final long maxBin = (long) Math.floor(((value.getMaximum().doubleValue() + offset) / interval));
    return LongStream.rangeClosed(minBin, maxBin).mapToObj(this::getBinId).toArray(
        ByteArray[]::new);
  }

  @Override
  public byte[] toBinary() {
    final byte[] parentBinary = super.toBinary();
    final ByteBuffer buf = ByteBuffer.allocate(parentBinary.length + 16);
    buf.put(parentBinary);
    buf.putDouble(interval);
    buf.putDouble(offset);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final byte[] parentBinary = new byte[bytes.length - 16];
    buf.get(parentBinary);
    super.fromBinary(parentBinary);
    interval = buf.getDouble();
    offset = buf.getDouble();
  }

  public Range<Double> getRange(final ByteArray bytes) {
    final Map<String, Range<Double>> allRanges = getRanges(bytes);
    final Optional<Range<Double>> mergedRange =
        allRanges.values().stream().filter(Objects::nonNull).reduce(
            (r1, r2) -> Range.between(
                Math.min(r1.getMinimum(), r2.getMinimum()),
                Math.max(r1.getMaximum(), r2.getMaximum())));
    if (mergedRange.isPresent()) {
      return mergedRange.get();
    }
    return null;
  }

  public Map<String, Range<Double>> getRanges(final ByteArray bytes) {
    return getRanges(ByteBuffer.wrap(bytes.getBytes()));
  }

  private Map<String, Range<Double>> getRanges(final ByteBuffer buffer) {
    final Map<String, Range<Double>> retVal = new HashMap<>();
    for (final String field : fields) {
      if (!buffer.hasRemaining()) {
        return retVal;
      }
      if (buffer.get() == 0x0) {
        retVal.put(field, null);
      } else {
        retVal.put(field, getRange(buffer));
        if (buffer.hasRemaining()) {
          buffer.getChar();
        }
      }
    }
    return retVal;
  }

  private Range<Double> getRange(final ByteBuffer buffer) {
    final byte[] longBuffer = new byte[Long.BYTES];
    buffer.get(longBuffer);
    final double low = (Lexicoders.LONG.fromByteArray(longBuffer) * interval) - offset;
    return Range.between(low, low + interval);
  }

  @Override
  public String binToString(final ByteArray bin) {
    final ByteBuffer buffer = ByteBuffer.wrap(bin.getBytes());
    final StringBuffer sb = new StringBuffer();
    while (buffer.remaining() > 0) {
      if (buffer.get() == 0x0) {
        sb.append("<null>");
      } else {
        sb.append(rangeToString(getRange(buffer)));
      }
      if (buffer.remaining() > 0) {
        sb.append(buffer.getChar());
      }
    }
    return sb.toString();
  }

  private static String rangeToString(final Range<Double> range) {
    final StringBuilder buf = new StringBuilder(32);
    buf.append('[');
    buf.append(range.getMinimum());
    buf.append("..");
    buf.append(range.getMaximum());
    buf.append(')');
    return buf.toString();
  }
}
