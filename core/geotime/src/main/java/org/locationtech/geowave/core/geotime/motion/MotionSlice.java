/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.motion;

/**
 * One piece of an entity's movement at one H3 level, emitted by {@link H3MotionSegmenter}.
 *
 * <p> Every slice is attributed to one {@link #getCell() cell} and one {@link #getBucketStart()
 * time bucket}, so a consumer can add it to a per (bucket, level, cell) aggregate without further
 * decisions. Times are epoch milliseconds truncated from the segmenter's sub-millisecond crossing
 * times, except that an occupancy piece's end is rounded up so the piece covers its interval;
 * durations and distances are exact.
 *
 * <p> Direction slots number a cell's neighbors: slot {@code d - 1} is the neighbor in H3 direction
 * digit {@code d} (1 to 6; a pentagon has no digit 1, so never slot 0), and {@link #ENDPOINT}
 * stands for the start or end of a track, a stop, or a gap.
 */
public final class MotionSlice<K> {
  public enum Type {
    /**
     * Time spent in the cell within one bucket, moving or stopped, and the distance covered.
     * Bridged occupancy is the straight chord across a short feasible gap.
     */
    OCCUPANCY,
    /** A visit began; attributed to its start. Stable identities only. */
    VISIT_START,
    /**
     * A visit ended; attributed to its end, which is when its statistics are known. Carries the
     * visit's totals, speed, heading, mean altitude and, for a moving traversal, the traversal
     * time. Censored when the visit was cut short by a gap, an eviction or the end of the data.
     * Stable identities only.
     */
    VISIT_END,
    /**
     * One passage through the cell from an entry slot to an exit slot, attributed to its end. A
     * visit contains one passage, or more when stops split it. Unstable identities emit only
     * passages from one neighbor to another.
     */
    TRANSITION,
    /** Continuity was lost after the entity's last report; attributed to that time and cell. */
    GAP_START,
    /**
     * A gap edge, emitted when the gap ends or, censored, when the entity is evicted or the data
     * ends. Attributed to the gap's from-cell and the bucket of its start; the other cell is where
     * the entity reappeared (0 when censored) and the duration is the observed lower bound when
     * censored.
     */
    GAP_END,
    /** The entity stopped. */
    STOP,
    /** The entity left a stop; attributed to the time of the last report inside the stop. */
    START
  }

  public static final int ENDPOINT = 6;
  public static final int NO_SLOT = -1;

  private final Type type;
  private final K entityKey;
  private final MotionDomain domain;
  private final boolean stableIdentity;
  private final int level;
  private final long cell;
  private final long bucketStart;
  private final long startTime;
  private final long endTime;
  private long otherCell;
  private boolean bridged;
  private boolean censored;
  private int entrySlot = NO_SLOT;
  private int exitSlot = NO_SLOT;
  private double movingSeconds;
  private double stoppedSeconds;
  private double distanceMeters;
  private double speed = Double.NaN;
  private double heading = Double.NaN;
  private double altitude = Double.NaN;
  private double traversalSeconds = Double.NaN;

  MotionSlice(
      final Type type,
      final K entityKey,
      final MotionDomain domain,
      final boolean stableIdentity,
      final int level,
      final long cell,
      final long bucketStart,
      final long startTime,
      final long endTime) {
    this.type = type;
    this.entityKey = entityKey;
    this.domain = domain;
    this.stableIdentity = stableIdentity;
    this.level = level;
    this.cell = cell;
    this.bucketStart = bucketStart;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  MotionSlice<K> withOccupancy(
      final double moving,
      final double stopped,
      final double distance,
      final boolean isBridged) {
    movingSeconds = moving;
    stoppedSeconds = stopped;
    distanceMeters = distance;
    bridged = isBridged;
    return this;
  }

  MotionSlice<K> withSlots(final int entry, final int exit) {
    entrySlot = entry;
    exitSlot = exit;
    return this;
  }

  MotionSlice<K> withVisit(
      final double moving,
      final double stopped,
      final double distance,
      final double visitSpeed,
      final double visitHeading,
      final double meanAltitude,
      final double traversal,
      final boolean isCensored) {
    movingSeconds = moving;
    stoppedSeconds = stopped;
    distanceMeters = distance;
    speed = visitSpeed;
    heading = visitHeading;
    altitude = meanAltitude;
    traversalSeconds = traversal;
    censored = isCensored;
    return this;
  }

  MotionSlice<K> withGap(final long toCell, final boolean isCensored) {
    otherCell = toCell;
    censored = isCensored;
    return this;
  }

  public Type getType() {
    return type;
  }

  public K getEntityKey() {
    return entityKey;
  }

  public MotionDomain getDomain() {
    return domain;
  }

  public boolean isStableIdentity() {
    return stableIdentity;
  }

  /** The H3 resolution of {@link #getCell()}. */
  public int getLevel() {
    return level;
  }

  public long getCell() {
    return cell;
  }

  /** For {@link Type#GAP_END}, the cell where the entity reappeared, or 0 when censored. */
  public long getOtherCell() {
    return otherCell;
  }

  public long getBucketStart() {
    return bucketStart;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public double getDurationSeconds() {
    return (endTime - startTime) / 1000.0;
  }

  public boolean isBridged() {
    return bridged;
  }

  public boolean isCensored() {
    return censored;
  }

  public int getEntrySlot() {
    return entrySlot;
  }

  public int getExitSlot() {
    return exitSlot;
  }

  public double getMovingSeconds() {
    return movingSeconds;
  }

  public double getStoppedSeconds() {
    return stoppedSeconds;
  }

  public double getDistanceMeters() {
    return distanceMeters;
  }

  /** Visit speed in m/s, distance over moving time; NaN when the visit never moved. */
  public double getSpeed() {
    return speed;
  }

  /**
   * Visit heading in degrees, the bearing from where the visit began to where it ended; NaN when
   * that displacement is within the source's tolerance plus sigma.
   */
  public double getHeading() {
    return heading;
  }

  /** Time-weighted mean altitude of the visit in meters; NaN when unknown. */
  public double getAltitude() {
    return altitude;
  }

  /**
   * Seconds from entry to exit of a visit that came from one neighbor, left to another and never
   * stopped; NaN otherwise.
   */
  public double getTraversalSeconds() {
    return traversalSeconds;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder().append(type).append(' ').append(entityKey).append(" L").append(
            level).append(' ').append(Long.toHexString(cell)).append(" [").append(startTime).append(
                ", ").append(endTime).append(']');
    if (type == Type.OCCUPANCY) {
      sb.append(" moving ").append(movingSeconds).append(" s stopped ").append(
          stoppedSeconds).append(" s ").append(distanceMeters).append(" m");
      if (bridged) {
        sb.append(" bridged");
      }
    }
    if (entrySlot != NO_SLOT || exitSlot != NO_SLOT) {
      sb.append(" slots ").append(entrySlot).append("->").append(exitSlot);
    }
    if (type == Type.GAP_END) {
      sb.append(" to ").append(Long.toHexString(otherCell));
    }
    if (censored) {
      sb.append(" censored");
    }
    return sb.toString();
  }
}
