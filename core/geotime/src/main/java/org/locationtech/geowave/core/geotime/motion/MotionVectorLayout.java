/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.motion;

import java.util.Arrays;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.statistics.adapter.AdditiveVectorStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.AdditiveVectorStatistic.AdditiveVectorValue;

/**
 * The positions of the motion vector, one additive value per (time bucket, level, cell), and the
 * histogram edges that bin it.
 *
 * <p><b>Counts</b> (84): {@link #VISITS} completed visits, censored ones included;
 * {@link #STOP_STARTS} stops that began here; {@link #GAP_STARTS} gaps that began here;
 * {@link #SPEED_HISTOGRAM} (8) visits by speed, distance over moving time; {@link #HEADING_ROSE}
 * (8) visits by heading sector, sector 0 centered on north, clockwise;
 * {@link #GAP_DURATION_HISTOGRAM} (6) gaps by duration; {@link #ALTITUDE_HISTOGRAM} (4) AIR visits
 * by mean altitude; {@link #EXIT_TRAVERSALS} (6) moving traversals by exit slot; {@link #FLOW} (49)
 * passages by entry slot x 7 + exit slot.
 *
 * <p><b>Sums</b> (20): {@link #MOVING_SECONDS} and {@link #STOPPED_SECONDS} occupancy of every
 * entity; {@link #STABLE_SECONDS} the moving plus stopped occupancy of stable identities only;
 * {@link #DISTANCE_METERS} distance covered; {@link #HEADING_COS} to {@link #HEADING_SIN2} (4) the
 * sums of cos t, sin t, cos 2t and sin 2t over visit headings t (compass bearings);
 * {@link #EXIT_TRAVERSAL_SECONDS} (6) moving traversal time by exit slot and
 * {@link #EXIT_TRAVERSAL_SECONDS_SQUARED} (6) its sum of squares.
 *
 * <p> Slots are those of {@link MotionSlice}: 0 to 5 for neighbor direction digits 1 to 6, and 6
 * for an endpoint (the start or end of a track, a stop, or a gap). Visit statistics are attributed
 * to the bucket of the visit's end, passages to the bucket of their end, and gap durations to the
 * gap's from-cell and the bucket of its start. For stable identities a cell's flow total in a
 * bucket is its visits plus its stop starts, since a stop ends one passage and begins another.
 * Unstable identities add occupancy and neighbor-to-neighbor passages but no visits, so dwell is
 * {@code STABLE_SECONDS / VISITS}. Only counts and sums are stored: dwell, means, variances and
 * probabilities are derived at query time.
 *
 * <p> The edges are configuration, not part of the schema, so changing them for an existing data
 * type mixes binnings; a changed layout needs a new data type (a new statistic). Every histogram
 * has one more bin than it has edges, and a value equal to an edge goes in the bin above it.
 */
public final class MotionVectorLayout {
  public static final int SCHEMA_ID = 1;
  public static final int SCHEMA_VERSION = 1;

  public static final int SLOTS = 7;
  public static final int SPEED_BINS = 8;
  public static final int HEADING_SECTORS = 8;
  public static final int GAP_DURATION_BINS = 6;
  public static final int ALTITUDE_BINS = 4;
  public static final int DIRECTIONS = 6;

  public static final int VISITS = 0;
  public static final int STOP_STARTS = 1;
  public static final int GAP_STARTS = 2;
  public static final int SPEED_HISTOGRAM = 3;
  public static final int HEADING_ROSE = SPEED_HISTOGRAM + SPEED_BINS;
  public static final int GAP_DURATION_HISTOGRAM = HEADING_ROSE + HEADING_SECTORS;
  public static final int ALTITUDE_HISTOGRAM = GAP_DURATION_HISTOGRAM + GAP_DURATION_BINS;
  public static final int EXIT_TRAVERSALS = ALTITUDE_HISTOGRAM + ALTITUDE_BINS;
  public static final int FLOW = EXIT_TRAVERSALS + DIRECTIONS;
  public static final int COUNT_LENGTH = FLOW + (SLOTS * SLOTS);

  public static final int MOVING_SECONDS = 0;
  public static final int STOPPED_SECONDS = 1;
  public static final int STABLE_SECONDS = 2;
  public static final int DISTANCE_METERS = 3;
  public static final int HEADING_COS = 4;
  public static final int HEADING_SIN = 5;
  public static final int HEADING_COS2 = 6;
  public static final int HEADING_SIN2 = 7;
  public static final int EXIT_TRAVERSAL_SECONDS = 8;
  public static final int EXIT_TRAVERSAL_SECONDS_SQUARED = EXIT_TRAVERSAL_SECONDS + DIRECTIONS;
  public static final int SUM_LENGTH = EXIT_TRAVERSAL_SECONDS_SQUARED + DIRECTIONS;

  /** In m/s: about 1, 3, 6, 10, 15, 20 and 30 knots. */
  private static final double[] DEFAULT_SURFACE_SPEED_EDGES = {0.5, 1.5, 3, 5, 7.5, 10, 15};
  private static final double[] DEFAULT_AIR_SPEED_EDGES = {25, 50, 100, 150, 200, 250, 300};
  private static final double[] DEFAULT_GROUND_SPEED_EDGES = {1, 3, 6, 10, 15, 20, 30};
  private static final double[] DEFAULT_UNKNOWN_SPEED_EDGES = {1, 3, 6, 10, 20, 50, 100};
  /** In seconds: 10 and 30 minutes, 1, 2 and 6 hours. */
  private static final double[] DEFAULT_GAP_DURATION_EDGES = {600, 1800, 3600, 7200, 21600};
  /** In meters: about 10,000, 25,000 and 35,000 feet. */
  private static final double[] DEFAULT_ALTITUDE_EDGES = {3048, 7620, 10668};

  private final double[] speedEdges;
  private final double[] gapDurationEdges;
  private final double[] altitudeEdges;

  public MotionVectorLayout(
      final double[] speedEdges,
      final double[] gapDurationEdges,
      final double[] altitudeEdges) {
    this.speedEdges = edges(speedEdges, SPEED_BINS);
    this.gapDurationEdges = edges(gapDurationEdges, GAP_DURATION_BINS);
    this.altitudeEdges = edges(altitudeEdges, ALTITUDE_BINS);
  }

  public static MotionVectorLayout defaults(final MotionDomain domain) {
    final double[] speeds;
    switch (domain) {
      case SURFACE:
        speeds = DEFAULT_SURFACE_SPEED_EDGES;
        break;
      case AIR:
        speeds = DEFAULT_AIR_SPEED_EDGES;
        break;
      case GROUND:
        speeds = DEFAULT_GROUND_SPEED_EDGES;
        break;
      default:
        speeds = DEFAULT_UNKNOWN_SPEED_EDGES;
        break;
    }
    return new MotionVectorLayout(speeds, DEFAULT_GAP_DURATION_EDGES, DEFAULT_ALTITUDE_EDGES);
  }

  private static double[] edges(final double[] edges, final int bins) {
    if (edges.length != (bins - 1)) {
      throw new IllegalArgumentException(bins + " bins need " + (bins - 1) + " edges");
    }
    for (int i = 1; i < edges.length; i++) {
      if (!(edges[i] > edges[i - 1])) {
        throw new IllegalArgumentException("Edges must be strictly ascending");
      }
    }
    return edges.clone();
  }

  public static int flowIndex(final int entrySlot, final int exitSlot) {
    return FLOW + (entrySlot * SLOTS) + exitSlot;
  }

  /** The rose sector of a compass bearing in degrees. */
  public static int headingSector(final double degrees) {
    final double sectorWidth = 360.0 / HEADING_SECTORS;
    final double shifted = (degrees + (sectorWidth / 2)) % 360;
    return (int) ((shifted < 0 ? shifted + 360 : shifted) / sectorWidth) % HEADING_SECTORS;
  }

  public int speedBin(final double metersPerSecond) {
    return bin(speedEdges, metersPerSecond);
  }

  public int gapDurationBin(final double seconds) {
    return bin(gapDurationEdges, seconds);
  }

  public int altitudeBin(final double meters) {
    return bin(altitudeEdges, meters);
  }

  private static int bin(final double[] edges, final double value) {
    int bin = 0;
    while ((bin < edges.length) && (value >= edges[bin])) {
      bin++;
    }
    return bin;
  }

  public double[] getSpeedEdges() {
    return speedEdges.clone();
  }

  public double[] getGapDurationEdges() {
    return gapDurationEdges.clone();
  }

  public double[] getAltitudeEdges() {
    return altitudeEdges.clone();
  }

  /**
   * An empty value of this layout, optionally bound to the statistic it will be written to. An
   * additive vector statistic supplies the value itself, so the value always carries the
   * statistic's schema; the statistic must hold this layout (schema id 1, at least this version and
   * these lengths).
   *
   * @throws IllegalArgumentException if the statistic holds a different layout
   */
  public AdditiveVectorValue createValue(final Statistic<?> statistic) {
    if (statistic instanceof AdditiveVectorStatistic) {
      final AdditiveVectorStatistic vector = (AdditiveVectorStatistic) statistic;
      if ((vector.getSchemaId() != SCHEMA_ID)
          || (vector.getSchemaVersion() < SCHEMA_VERSION)
          || (vector.getCountLength() < COUNT_LENGTH)
          || (vector.getSumLength() < SUM_LENGTH)) {
        throw new IllegalArgumentException(
            "The statistic holds schema "
                + vector.getSchemaId()
                + " v"
                + vector.getSchemaVersion()
                + " with "
                + vector.getCountLength()
                + " counts and "
                + vector.getSumLength()
                + " sums, not the motion layout");
      }
      return vector.createEmpty();
    }
    return new AdditiveVectorValue(statistic, SCHEMA_ID, SCHEMA_VERSION, COUNT_LENGTH, SUM_LENGTH);
  }

  /**
   * A statistic for {@code typeName} holding values of this layout; binning is up to the caller.
   */
  public static AdditiveVectorStatistic createStatistic(final String typeName) {
    return new AdditiveVectorStatistic(
        typeName,
        SCHEMA_ID,
        SCHEMA_VERSION,
        COUNT_LENGTH,
        SUM_LENGTH);
  }

  @Override
  public String toString() {
    return "MotionVectorLayout [speedEdges="
        + Arrays.toString(speedEdges)
        + ", gapDurationEdges="
        + Arrays.toString(gapDurationEdges)
        + ", altitudeEdges="
        + Arrays.toString(altitudeEdges)
        + "]";
  }
}
