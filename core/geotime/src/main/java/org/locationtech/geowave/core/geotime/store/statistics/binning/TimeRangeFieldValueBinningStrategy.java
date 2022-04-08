/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.statistics.binning;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalDimensionalityTypeProvider.UnitConverter;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.dimension.bin.BinRange;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.statistics.binning.FieldValueBinningStrategy;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl.ExplicitConstraints;
import org.threeten.extra.Interval;
import com.beust.jcommander.Parameter;

/**
 * Statistic binning strategy that bins statistic values by the temporal representation of the value
 * of a given field. It bins time values by a temporal periodicity (any time unit), default to
 * daily. A statistic using this binning strategy can be constrained using
 * org.threeten.extra.Interval class as a constraint).
 */
public class TimeRangeFieldValueBinningStrategy extends FieldValueBinningStrategy {
  protected static Unit DEFAULT_PERIODICITY = Unit.DAY;
  public static final String NAME = "TIME_RANGE";

  @Parameter(
      names = {"--binInteval"},
      required = false,
      description = "The interval or periodicity at which to bin time values.  Defaults to daily.",
      converter = UnitConverter.class)
  protected Unit periodicity = DEFAULT_PERIODICITY;

  @Parameter(
      names = {"-tz", "--timezone"},
      required = false,
      description = "The timezone to convert all incoming time values into. Defaults to GMT.")
  protected String timezone = "GMT";

  private TemporalBinningStrategy binningStrategy;

  @Override
  public String getStrategyName() {
    return NAME;
  }

  public TimeRangeFieldValueBinningStrategy() {
    super();
  }

  public TimeRangeFieldValueBinningStrategy(final String... fields) {
    super(fields);
  }

  public TimeRangeFieldValueBinningStrategy(final Unit periodicity, final String... fields) {
    this("GMT", periodicity, fields);
  }

  public TimeRangeFieldValueBinningStrategy(
      final String timezone,
      final Unit periodicity,
      final String... fields) {
    super(fields);
    this.periodicity = periodicity;
    this.timezone = timezone;
    binningStrategy = new TemporalBinningStrategy(periodicity, timezone);
  }


  @Override
  public String getDescription() {
    return "Bin the statistic by the time value of a specified field.";
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<?>[] supportedConstraintClasses() {
    return ArrayUtils.addAll(
        super.supportedConstraintClasses(),
        Date.class,
        Calendar.class,
        Number.class,
        Interval.class,
        Interval[].class);
  }

  @Override
  public ByteArrayConstraints singleFieldConstraints(final Object constraint) {
    if (constraint instanceof Interval) {
      return new ExplicitConstraints(getNumericBins((Interval) constraint));
    } else if (constraint instanceof Interval[]) {
      final Stream<ByteArray[]> stream =
          Arrays.stream((Interval[]) constraint).map(this::getNumericBins);
      return new ExplicitConstraints(stream.flatMap(Arrays::stream).toArray(ByteArray[]::new));
    }
    final long timeMillis = TimeUtils.getTimeMillis(constraint);
    if (timeMillis != TimeUtils.RESERVED_MILLIS_FOR_NULL) {
      return new ExplicitConstraints(new ByteArray[] {getTimeBin(timeMillis)});
    }
    return super.constraints(constraint);
  }

  @Override
  protected ByteArray getSingleBin(final Object value) {
    final long millis = TimeUtils.getTimeMillis(value);
    if (millis == TimeUtils.RESERVED_MILLIS_FOR_NULL) {
      return new ByteArray();
    }
    return getTimeBin(millis);
  }

  private ByteArray getTimeBin(final long millis) {
    return new ByteArray(binningStrategy.getBinId(millis));
  }

  private ByteArray[] getNumericBins(final Interval value) {
    final BinRange[] bins = binningStrategy.getNormalizedRanges(value);
    return Arrays.stream(bins).map(BinRange::getBinId).map(ByteArray::new).toArray(
        ByteArray[]::new);
  }

  @Override
  public byte[] toBinary() {
    final byte[] parentBinary = super.toBinary();
    final byte[] timezoneBytes = StringUtils.stringToBinary(timezone);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            parentBinary.length
                + VarintUtils.unsignedIntByteLength(periodicity.ordinal())
                + VarintUtils.unsignedIntByteLength(timezoneBytes.length)
                + timezoneBytes.length);
    VarintUtils.writeUnsignedInt(periodicity.ordinal(), buf);
    VarintUtils.writeUnsignedInt(timezoneBytes.length, buf);
    buf.put(timezoneBytes);
    buf.put(parentBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    periodicity = Unit.values()[VarintUtils.readUnsignedInt(buf)];
    final byte[] timezoneBinary = new byte[VarintUtils.readUnsignedInt(buf)];
    buf.get(timezoneBinary);
    timezone = StringUtils.stringFromBinary(timezoneBinary);
    binningStrategy = new TemporalBinningStrategy(periodicity, timezone);
    final byte[] parentBinary = new byte[buf.remaining()];
    buf.get(parentBinary);
    super.fromBinary(parentBinary);
  }

  public Interval getInterval(final ByteArray binId) {
    return getInterval(binId.getBytes());
  }

  private Interval getInterval(final byte[] binId) {
    return binningStrategy.getInterval(binId);
  }

  @Override
  public String binToString(final ByteArray bin) {
    final ByteBuffer buffer = ByteBuffer.wrap(bin.getBytes());
    final StringBuffer sb = new StringBuffer();
    while (buffer.remaining() > 0) {
      if (buffer.get() == 0x0) {
        sb.append("<null>");
      } else {
        final byte[] binId = new byte[binningStrategy.getFixedBinIdSize()];
        buffer.get(binId);
        sb.append(getInterval(binId).toString());
      }
      if (buffer.remaining() > 0) {
        sb.append(buffer.getChar());
      }
    }
    return sb.toString();
  }
}
