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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import java.util.TreeSet;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.core.geotime.binning.H3CellRanges;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayRange.MergeOperation;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.binning.FieldValueBinningStrategy;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl.ExplicitConstraints;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

/**
 * Statistic binning strategy that bins by the H3 cell, at a fixed resolution, containing the H3
 * cell id held in a single field. Each bin is the cell id encoded as an 8 byte sortable long, so a
 * geometry or a coarser cell maps to contiguous bin ranges.
 */
public class H3CellFieldValueBinningStrategy extends FieldValueBinningStrategy {
  public static final String NAME = "H3_CELL";
  private static final int MAX_RESOLUTION = 15;

  @Parameter(
      names = {"--binResolution"},
      required = false,
      validateValueWith = ResolutionValidator.class,
      description = "The H3 resolution (0-15) of the bins.")
  protected int resolution = 8;

  public static class ResolutionValidator implements IValueValidator<Integer> {
    @Override
    public void validate(final String name, final Integer value) throws ParameterException {
      if ((value < 0) || (value > MAX_RESOLUTION)) {
        throw new ParameterException(
            "Parameter " + name + " must be between 0 and " + MAX_RESOLUTION + ", got " + value);
      }
    }
  }

  public H3CellFieldValueBinningStrategy() {
    super();
  }

  public H3CellFieldValueBinningStrategy(final int resolution, final String field) {
    super(field);
    this.resolution = validateResolution(resolution);
  }

  private static int validateResolution(final int resolution) {
    if ((resolution < 0) || (resolution > MAX_RESOLUTION)) {
      throw new IllegalArgumentException(
          "H3 resolution must be between 0 and " + MAX_RESOLUTION + ", got " + resolution);
    }
    return resolution;
  }

  @Override
  public String getStrategyName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return "Bin the statistic by the H3 cell, at a fixed resolution, of the H3 cell id in a specified field.";
  }

  public int getResolution() {
    return resolution;
  }

  @Override
  public OptionalInt getFixedBinLength() {
    return OptionalInt.of(Long.BYTES);
  }

  private void requireValidConfiguration() {
    validateResolution(resolution);
    if (fields.size() > 1) {
      throw new IllegalArgumentException(
          NAME
              + " bins by exactly one field, but --binField was given "
              + fields.size()
              + " times");
    }
  }

  @Override
  public String getDefaultTag() {
    return super.getDefaultTag() + "-H3(" + resolution + ")";
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<?>[] supportedConstraintClasses() {
    return ArrayUtils.addAll(
        super.supportedConstraintClasses(),
        Geometry.class,
        Geometry[].class,
        Envelope.class,
        Envelope[].class,
        Long.class,
        long[].class,
        Long[].class,
        Collection.class);
  }

  @Override
  public <T> ByteArray[] getBins(
      final DataTypeAdapter<T> adapter,
      final T entry,
      final GeoWaveRow... rows) {
    if (fields.isEmpty()) {
      return new ByteArray[0];
    }
    requireValidConfiguration();
    final Long cell = toCell(adapter.getFieldValue(entry, fields.get(0)));
    if ((cell == null) || (H3CellRanges.h3().getResolution(cell) < resolution)) {
      return new ByteArray[0];
    }
    return new ByteArray[] {getBin(cell)};
  }

  @Override
  public ByteArrayConstraints constraints(final Object constraint) {
    requireValidConfiguration();
    return super.constraints(constraint);
  }

  @Override
  public ByteArrayConstraints singleFieldConstraints(final Object constraint) {
    final List<ByteArrayRange> ranges = new ArrayList<>();
    final TreeSet<ByteArray> bins = new TreeSet<>();
    if (constraint instanceof Geometry) {
      addGeometry((Geometry) constraint, ranges);
    } else if (constraint instanceof Geometry[]) {
      for (final Geometry geometry : (Geometry[]) constraint) {
        addGeometry(geometry, ranges);
      }
    } else if (constraint instanceof Envelope) {
      addGeometry(GeometryUtils.GEOMETRY_FACTORY.toGeometry((Envelope) constraint), ranges);
    } else if (constraint instanceof Envelope[]) {
      for (final Envelope envelope : (Envelope[]) constraint) {
        addGeometry(GeometryUtils.GEOMETRY_FACTORY.toGeometry(envelope), ranges);
      }
    } else if ((constraint instanceof Number)
        || (constraint instanceof String)
        || (constraint instanceof long[])
        || (constraint instanceof Long[])
        || (constraint instanceof String[])
        || (constraint instanceof Collection)) {
      addCells(constraint, ranges, bins);
    } else {
      return super.singleFieldConstraints(constraint);
    }
    if (ranges.isEmpty()) {
      return new ExplicitConstraints(bins.toArray(new ByteArray[0]));
    }
    for (final ByteArray bin : bins) {
      ranges.add(new ByteArrayRange(bin.getBytes(), bin.getBytes()));
    }
    // some stores scan each range separately, so overlapping ranges would read values twice
    return new ExplicitConstraints(
        ByteArrayRange.mergeIntersections(ranges, MergeOperation.UNION).toArray(
            new ByteArrayRange[0]));
  }

  private void addGeometry(final Geometry geometry, final List<ByteArrayRange> ranges) {
    ranges.addAll(Arrays.asList(H3CellRanges.geometryToRanges(geometry, resolution)));
  }

  private void addCells(
      final Object constraint,
      final List<ByteArrayRange> ranges,
      final TreeSet<ByteArray> bins) {
    final List<Object> cells = new ArrayList<>();
    if (constraint instanceof long[]) {
      Arrays.stream((long[]) constraint).forEach(cells::add);
    } else if (constraint instanceof Object[]) {
      cells.addAll(Arrays.asList((Object[]) constraint));
    } else if (constraint instanceof Collection) {
      cells.addAll((Collection<?>) constraint);
    } else {
      cells.add(constraint);
    }
    for (final Object value : cells) {
      final Long cell = toCell(value);
      if (cell == null) {
        throw new IllegalArgumentException("Not a valid H3 cell id: " + value);
      }
      if (H3CellRanges.h3().getResolution(cell) >= resolution) {
        bins.add(getBin(cell));
      } else {
        ranges.add(H3CellRanges.descendantRange(cell, resolution));
      }
    }
  }

  // null for anything that is not a valid H3 cell
  private static Long toCell(final Object value) {
    final long cell;
    if (value instanceof Number) {
      cell = ((Number) value).longValue();
    } else if (value instanceof String) {
      try {
        cell = H3CellRanges.h3().stringToH3((String) value);
      } catch (final RuntimeException e) {
        return null;
      }
    } else {
      return null;
    }
    return H3CellRanges.h3().isValidCell(cell) ? cell : null;
  }

  public ByteArray getBin(final long cell) {
    return H3CellRanges.toBin(H3CellRanges.h3().cellToParent(cell, resolution));
  }

  public long getCell(final ByteArray bin) {
    return H3CellRanges.fromBin(bin.getBytes());
  }

  public Geometry getBinGeometry(final ByteArray bin) {
    return H3CellRanges.cellToGeometry(getCell(bin));
  }

  @Override
  public String binToString(final ByteArray bin) {
    return H3CellRanges.h3().h3ToString(getCell(bin));
  }

  @Override
  public byte[] toBinary() {
    requireValidConfiguration();
    final byte[] parentBinary = super.toBinary();
    final ByteBuffer buf =
        ByteBuffer.allocate(VarintUtils.unsignedIntByteLength(resolution) + parentBinary.length);
    VarintUtils.writeUnsignedInt(resolution, buf);
    buf.put(parentBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    resolution = VarintUtils.readUnsignedInt(buf);
    final byte[] parentBinary = new byte[buf.remaining()];
    buf.get(parentBinary);
    super.fromBinary(parentBinary);
    requireValidConfiguration();
  }
}
