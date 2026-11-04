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

/**
 * The segmenter's state for one entity: about 420 bytes with two levels. Index 0 of the per-level
 * arrays is the crossing level, the finest the entity contributes to.
 */
final class MotionEntity<K> {
  final K key;
  final boolean stable;
  final MotionProfile profile;

  EntityHeap<K> heap;
  int heapIndex = -1;
  /** When the entity times out (active) or is evicted (in a gap), if nothing arrives before. */
  long deadline;

  long lastTime;
  /** The path position at lastTime: the last report, or the anchor while stopped. */
  double latitude;
  double longitude;
  /** Where the last report actually was, which differs from the path position while stopped. */
  double reportLatitude;
  double reportLongitude;
  double altitude;

  boolean inGap;
  long gapStart;
  boolean stopped;
  int fastReports;
  long fastSince;
  /** The start of the current baseline for reports without a speed. */
  long baseTime;
  double baseLatitude;
  double baseLongitude;

  /** The cell the path is in at each level; while in a gap, the cells it was last seen in. */
  final long[] cells;
  /** The open visit at each level; it lags the path while an excursion is pending. */
  final Visit[] visits;
  /** The pending excursion at each level, if any. */
  final Excursion[] excursions;

  MotionEntity(final K key, final boolean stable, final MotionProfile profile) {
    this.key = key;
    this.stable = stable;
    this.profile = profile;
    cells = new long[profile.levels.length];
    visits = new Visit[profile.levels.length];
    excursions = new Excursion[profile.levels.length];
    for (int i = 0; i < visits.length; i++) {
      visits[i] = new Visit();
    }
  }

  /** An indexed binary min-heap of entities by deadline. */
  static final class EntityHeap<K> {
    private MotionEntity<K>[] entities = newArray(16);
    private int size;

    @SuppressWarnings("unchecked")
    private static <K> MotionEntity<K>[] newArray(final int length) {
      return new MotionEntity[length];
    }

    MotionEntity<K> peek() {
      return size == 0 ? null : entities[0];
    }

    void add(final MotionEntity<K> entity) {
      if (size == entities.length) {
        entities = Arrays.copyOf(entities, size * 2);
      }
      entity.heap = this;
      place(entity, size++);
      siftUp(entity.heapIndex);
    }

    void remove(final MotionEntity<K> entity) {
      final int index = entity.heapIndex;
      final MotionEntity<K> last = entities[--size];
      entities[size] = null;
      if (index != size) {
        place(last, index);
        siftDown(index);
        siftUp(last.heapIndex);
      }
      entity.heap = null;
      entity.heapIndex = -1;
    }

    /** Restores the order after the entity's deadline changed. */
    void update(final MotionEntity<K> entity) {
      siftDown(entity.heapIndex);
      siftUp(entity.heapIndex);
    }

    private void place(final MotionEntity<K> entity, final int index) {
      entities[index] = entity;
      entity.heapIndex = index;
    }

    private void siftUp(final int from) {
      int index = from;
      final MotionEntity<K> entity = entities[index];
      while (index > 0) {
        final int parent = (index - 1) >>> 1;
        if (entities[parent].deadline <= entity.deadline) {
          break;
        }
        place(entities[parent], index);
        index = parent;
      }
      place(entity, index);
    }

    private void siftDown(final int from) {
      int index = from;
      final MotionEntity<K> entity = entities[index];
      while (true) {
        int child = (2 * index) + 1;
        if (child >= size) {
          break;
        }
        if (((child + 1) < size) && (entities[child + 1].deadline < entities[child].deadline)) {
          child++;
        }
        if (entity.deadline <= entities[child].deadline) {
          break;
        }
        place(entities[child], index);
        index = child;
      }
      place(entity, index);
    }
  }

  static class Totals {
    double movingSeconds;
    double stoppedSeconds;
    double distanceMeters;
    double altitudeSum;
    double altitudeSeconds;

    void add(
        final double moving,
        final double stopped,
        final double distance,
        final double altitude) {
      movingSeconds += moving;
      stoppedSeconds += stopped;
      distanceMeters += distance;
      if (!Double.isNaN(altitude)) {
        altitudeSum += altitude * (moving + stopped);
        altitudeSeconds += moving + stopped;
      }
    }

    void add(final Totals other) {
      movingSeconds += other.movingSeconds;
      stoppedSeconds += other.stoppedSeconds;
      distanceMeters += other.distanceMeters;
      altitudeSum += other.altitudeSum;
      altitudeSeconds += other.altitudeSeconds;
    }

    void clear() {
      movingSeconds = 0;
      stoppedSeconds = 0;
      distanceMeters = 0;
      altitudeSum = 0;
      altitudeSeconds = 0;
    }
  }

  static final class Visit extends Totals {
    long cell;
    double startTime;
    int entrySlot;
    /** The entry of the current passage, which a stop resets to the endpoint slot. */
    int passageEntrySlot;
    double entryLatitude;
    double entryLongitude;

    void begin(
        final long visitCell,
        final double time,
        final int slot,
        final double latitude,
        final double longitude) {
      cell = visitCell;
      startTime = time;
      entrySlot = slot;
      passageEntrySlot = slot;
      entryLatitude = latitude;
      entryLongitude = longitude;
      clear();
    }
  }

  /**
   * A move into a neighbor that may turn out to be a shallow excursion. Until it is resolved the
   * visit stays with the cell it left, and the time spent in the neighbor is held here.
   */
  static final class Excursion extends Totals {
    final long fromCell;
    final long cell;
    final double time;
    final double latitude;
    final double longitude;
    final int entrySlot;
    /** The cells' shared edge as unit vectors, or null when they are not neighbors. */
    private final double[][] edge;
    double depthMeters;

    Excursion(
        final long fromCell,
        final long cell,
        final double time,
        final double latitude,
        final double longitude,
        final int entrySlot,
        final double[][] edge) {
      this.fromCell = fromCell;
      this.cell = cell;
      this.time = time;
      this.latitude = latitude;
      this.longitude = longitude;
      this.entrySlot = entrySlot;
      this.edge = edge;
      depthMeters = edge == null ? Double.POSITIVE_INFINITY : 0;
    }

    void deepen(final double[] point) {
      if (edge == null) {
        return;
      }
      double distance = Double.POSITIVE_INFINITY;
      for (int i = 1; i < edge.length; i++) {
        distance = Math.min(distance, Spherical.distanceToArc(point, edge[i - 1], edge[i]));
      }
      depthMeters = Math.max(depthMeters, distance);
    }
  }
}
