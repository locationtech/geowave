/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.motion;

import static org.locationtech.geowave.core.geotime.motion.GapPolicy.saturatedAdd;
import static org.locationtech.geowave.core.geotime.motion.GapPolicy.saturatedSubtract;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;
import org.locationtech.geowave.core.geotime.motion.MotionEntity.EntityHeap;
import org.locationtech.geowave.core.geotime.motion.MotionEntity.Excursion;
import org.locationtech.geowave.core.geotime.motion.MotionEntity.Totals;
import org.locationtech.geowave.core.geotime.motion.MotionEntity.Visit;
import org.locationtech.geowave.core.geotime.motion.MotionSlice.Type;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;

/**
 * Turns thinned position reports into {@link MotionSlice}s on H3 cells: where each entity spent its
 * time, how it passed through each cell, and where it went dark. Reports are expected to have
 * passed dead-reckoning admission (see {@link MotionSource}), so the straight chord between two
 * consecutive reports is a faithful path. Occupancy and distance are measured along that path, and
 * excursions are judged against allowances wider than the thinning tolerance, so thinned and
 * unthinned traffic agree within the plan's tolerances on per-cell occupancy, visit speed, heading
 * and transition probabilities. They are not identical: when a shallow excursion lasts about its
 * window, thinned chords can cut its corner and merge it where dense input makes it a visit of its
 * own, and stop starts depend somewhat on how densely a feed reports speed near the stop
 * thresholds.
 *
 * <p><b>Path.</b> Consecutive reports within the source's continuity window are joined by the
 * great-circle chord, with time spread evenly along it. A longer separation or an implausible chord
 * is a gap ({@link GapPolicy}): no occupancy or transitions across it, only a gap edge, plus
 * bridged occupancy along the chord when the gap is short and feasible. While stopped the position
 * is anchored, so position jitter cannot cross a cell boundary.
 *
 * <p><b>Cells.</b> Crossings are computed at the finest level the source contributes to by sampling
 * the chord every half edge and bisecting each change of cell to within a centimeter. A
 * great-circle arc is straight in H3's gnomonic face projections, so two samples in the same convex
 * cell have nothing between them. Cells at the Class III levels (odd resolutions) that straddle an
 * icosahedron edge, pentagons and their neighbors need not be convex, so a same-cell sample
 * interval in one of those is sampled again every thousandth of an edge (at least a meter) first.
 * Coarser levels are the de-duplicated runs of {@code cellToParent} of that sequence, so a coarse
 * cell's occupancy is exactly the sum over its fine children. Unit-vector geometry keeps all of
 * this valid across the antimeridian and at the poles.
 *
 * <p><b>Visits.</b> A visit is a stay in one cell at one level. A move into a neighbor that returns
 * to the cell it left, with no sampled position deeper than an allowance from the two cells' shared
 * edge, is merged back into one visit; occupancy still follows the path. At the crossing level the
 * allowance is the tolerance (or sigma, if larger), the positions are the reports and the return
 * must come within a continuity window. At a derived level, whose cells have jagged boundaries, the
 * allowance is a fine-cell diameter plus that tolerance, the edge is the coarse hexagons', the
 * positions also include the fine-level crossings and the window is scaled by the ratio of edge
 * lengths, so a straight track crossing or skirting a jagged boundary makes no U-turn if it covers
 * the allowance within that window. A slower one still can: the cutoff speed is the allowance over
 * the scaled window, for example 1.5 m/s at levels 6 and 4 for a 200 m, 10 minute source (1.1 m/s
 * with a 3 minute raw interval) and 4.0 m/s at levels 5 and 3. A pending excursion becomes a visit
 * as soon as it is deeper than the allowance or has lasted its window, or when the path enters a
 * third cell, stops or loses continuity, so a censored visit lies in the cell its gap starts from;
 * when the track ends it is merged, since it never went deep. Entry and exit slots come from
 * {@code cellsToDirectedEdge}; when two consecutive cells are not neighbors, which H3 does not rule
 * out at a derived level, the slot is the neighbor whose center is nearest the other cell's center.
 * A stop splits a visit into passages at the endpoint slot, so the transition matrix separates flow
 * through a cell from trips that end or begin in it. Visits cut short by a gap, an eviction or
 * {@link #close()} are censored.
 *
 * <p><b>Identities.</b> Entities with unstable identities contribute occupancy (moving, stopped and
 * bridged) and neighbor-to-neighbor transitions only; no visits, stops, gaps or dwell. They are
 * kept in a gap only as long as a reappearance could still be bridged.
 *
 * <p><b>Ordering and state.</b> Reports for one entity must arrive in time order; one at or before
 * the entity's last processed time is dropped and counted ({@link #getOutOfOrderReports()}).
 * Entities of different keys may interleave freely. One key should always arrive with one
 * {@link MotionSource}. Callers must reject reports dated in the future of their own clock before
 * {@code accept}, because a report time advances the processed time for every entity. Each entity
 * takes a few hundred bytes. An entity is put in a gap by {@link #advanceTo(long)} once its last
 * report is older than its continuity window plus its source's allowed lateness (by
 * {@link #close()} once older than the window alone), and is evicted, its gap censored, once idle
 * longer than the idle TTL or when the entity limit is reached (preferring entities already in a
 * gap).
 *
 * <p><b>Watermark.</b> {@link #getCompleteThrough()} is the processed time minus the largest
 * continuity window plus lateness among the sources seen so far or declared with
 * {@link Builder#expectSource}; it stays at Long.MIN_VALUE until one is known. Read right after
 * {@code advanceTo}, it bounds later slices: occupancy, stop and gap-start slices fall at or after
 * it, as do visits and passages except those held back by a pending excursion: rarely at the
 * crossing level (a path skimming an edge for minutes), routinely at derived levels, where deciding
 * that a jagged boundary was really crossed takes up to a fine-cell diameter of travel.
 * {@link #getCompleteThrough(int)} backs off by the longest such hold at a level, so it bounds
 * every slice of that level except these, which a consumer should treat as late corrections:
 * reports that trail others by more than their lateness; GAP_END slices, including those of
 * evictions and {@link #close()}, since they are attributed to the gap's start; and bridged
 * occupancy, which fills a gap after the fact. A watermark never moves back, so a source first seen
 * after it has advanced, with a longer timeout or level hold than the sources already known, can
 * put slices behind it; declare expected sources to avoid that.
 *
 * <p> Not thread-safe.
 */
public class H3MotionSegmenter<K> {
  /** Hour buckets, the default. */
  public static final LongUnaryOperator HOUR = t -> Math.floorDiv(t, 3_600_000L) * 3_600_000L;

  private static final double CROSSING_PRECISION_METERS = 0.01;
  private static final int MAX_BISECTIONS = 64;
  private static final int DISTORTION_CACHE_BITS = 10;

  private final Consumer<? super MotionSlice<K>> consumer;
  private final GapPolicy gapPolicy;
  private final ResolutionPlan resolutionPlan;
  private final LongUnaryOperator bucketFunction;
  private final int maxEntities;
  private final long idleTtlMillis;
  private final H3Core h3 = H3Holder.h3();

  private final Map<K, MotionEntity<K>> entities = new HashMap<>();
  private final EntityHeap<K> active = new EntityHeap<>();
  private final EntityHeap<K> dormant = new EntityHeap<>();
  private final Map<MotionDomain, Map<MotionSource, MotionProfile>> profiles =
      new EnumMap<>(MotionDomain.class);

  private long processedTime = Long.MIN_VALUE;
  private long completeThrough = Long.MIN_VALUE;
  private long maxTimeoutMillis;
  private final long[] levelHoldMillis = new long[16];
  private final long[] levelCompleteThrough = new long[16];
  private long outOfOrderReports;
  private long invalidReports;
  private long unplacedReports;
  private long implausibleJumps;
  private long evictions;
  private long restarts;
  private long nonAdjacentSlots;

  private final double[] start = new double[3];
  private final double[] end = new double[3];
  private final double[] probe = new double[3];
  private final double[] point = new double[3];
  private double[] crossingS = new double[8];
  private long[] crossingFrom = new long[8];
  private long[] crossingTo = new long[8];
  private int crossingCount;
  private final long[] distortionKeys = new long[1 << DISTORTION_CACHE_BITS];
  private final boolean[] distortionValues = new boolean[1 << DISTORTION_CACHE_BITS];

  private H3MotionSegmenter(final Builder<K> builder) {
    Arrays.fill(levelCompleteThrough, Long.MIN_VALUE);
    consumer = builder.consumer;
    gapPolicy = builder.gapPolicy;
    resolutionPlan = builder.resolutionPlan;
    bucketFunction = builder.bucketFunction;
    maxEntities = builder.maxEntities;
    idleTtlMillis = builder.idleTtlMillis;
    builder.expectedSources.forEach(expected -> profile(expected.domain, expected.source));
  }

  public static <K> Builder<K> builder(final Consumer<? super MotionSlice<K>> consumer) {
    return new Builder<>(consumer);
  }

  public void accept(final MotionReport<K> report) {
    final double latitude = report.getLatitude();
    final double longitude = report.getLongitude();
    if (!(Math.abs(latitude) <= 90) || !Double.isFinite(longitude)) {
      invalidReports++;
      return;
    }
    final long time = report.getTime();
    if (time > processedTime) {
      processedTime = time;
    }
    final MotionProfile profile = profile(report.getDomain(), report.getSource());
    MotionEntity<K> entity = entities.get(report.getEntityKey());
    if ((entity != null) && (entity.profile != profile)) {
      restarts++;
      finish(entity, false);
      remove(entity);
      entity = null;
    }
    if (profile.levels.length == 0) {
      unplacedReports++;
    } else if (entity == null) {
      makeRoom();
      entity = new MotionEntity<>(report.getEntityKey(), report.isStableIdentity(), profile);
      entities.put(entity.key, entity);
      begin(entity, report, cellsAt(profile, latitude, longitude));
      entity.deadline = saturatedAdd(time, profile.timeoutMillis);
      active.add(entity);
    } else if (time <= entity.lastTime) {
      outOfOrderReports++;
    } else if (entity.inGap) {
      dormant.remove(entity);
      reappear(entity, report);
      entity.deadline = saturatedAdd(time, profile.timeoutMillis);
      active.add(entity);
    } else {
      final long elapsed = time - entity.lastTime;
      final double jump =
          Spherical.distance(entity.reportLatitude, entity.reportLongitude, latitude, longitude);
      final boolean plausible =
          gapPolicy.isPlausible(profile.domain, profile.source, jump, elapsed);
      if (!plausible) {
        implausibleJumps++;
      }
      if (!plausible || (elapsed > profile.windowMillis)) {
        startGap(entity);
        reappear(entity, report);
      } else {
        advance(entity, report, elapsed);
      }
      entity.deadline = saturatedAdd(time, profile.timeoutMillis);
      active.update(entity);
    }
    updateCompleteThrough();
  }

  /**
   * Ends an entity's track, for example when its source reports it terminated. Its open visits end
   * at its last report, uncensored.
   */
  public void end(final K entityKey) {
    final MotionEntity<K> entity = entities.get(entityKey);
    if (entity != null) {
      finish(entity, false);
      remove(entity);
    }
  }

  /**
   * Moves the processed time to at least {@code time}, puts every entity whose continuity window
   * plus lateness has passed in a gap, and evicts entities idle for longer than their TTL. Call it
   * periodically when streaming.
   */
  public void advanceTo(final long time) {
    if (time > processedTime) {
      processedTime = time;
    }
    final long now = processedTime;
    MotionEntity<K> entity;
    while (((entity = active.peek()) != null) && (entity.deadline < now)) {
      active.remove(entity);
      startGap(entity);
      entity.deadline = saturatedAdd(entity.gapStart, ttl(entity));
      dormant.add(entity);
    }
    while (((entity = dormant.peek()) != null) && (entity.deadline < now)) {
      evictions++;
      evict(entity);
    }
    updateCompleteThrough();
  }

  /**
   * Ends everything, for batch use: entities whose last report is older than their continuity
   * window get a gap, whatever their source's lateness, since no report can arrive after close.
   * Then every open visit ends censored at its entity's last report and every open gap ends
   * censored at the processed time.
   */
  public void close() {
    if (processedTime != Long.MIN_VALUE) {
      advanceTo(processedTime);
    }
    MotionEntity<K> entity;
    while ((entity = active.peek()) != null) {
      if ((processedTime - entity.lastTime) > entity.profile.windowMillis) {
        startGap(entity);
      }
      finish(entity, true);
      remove(entity);
    }
    while ((entity = dormant.peek()) != null) {
      finish(entity, true);
      remove(entity);
    }
  }

  /**
   * Read right after {@link #advanceTo(long)}, slices emitted from then on fall at or after this
   * time, with the exceptions described on the class. Read after {@link #accept(MotionReport)}
   * instead, it may already be past the last report of an entity that the next advanceTo puts in a
   * gap. Long.MIN_VALUE until a source is known, from a report or declared with
   * {@link Builder#expectSource}.
   */
  public long getCompleteThrough() {
    return completeThrough;
  }

  /**
   * Read right after {@link #advanceTo(long)}, slices of {@code level} emitted from then on fall at
   * or after this time, except gap ends, bridged occupancy and reports later than their lateness.
   * It allows for visits held back by a pending excursion, so a writer can close a level's buckets
   * with it. Read between advanceTo calls it still holds while advanceTo runs at least once per
   * continuity window: only advanceTo puts a silent entity in a gap, and the longer that waits, the
   * further behind the value read its gap start and held-back visit fall. Long.MIN_VALUE until a
   * source is known, from a report or declared with {@link Builder#expectSource}.
   */
  public long getCompleteThrough(final int level) {
    return levelCompleteThrough[level];
  }

  /** The latest report time or {@link #advanceTo(long)} time seen. */
  public long getProcessedTime() {
    return processedTime;
  }

  public int getEntityCount() {
    return entities.size();
  }

  public long getOutOfOrderReports() {
    return outOfOrderReports;
  }

  /** Reports with a latitude outside [-90, 90] or a non-finite longitude. */
  public long getInvalidReports() {
    return invalidReports;
  }

  /** Reports from a source too coarse for every stored level of its domain. */
  public long getUnplacedReports() {
    return unplacedReports;
  }

  public long getImplausibleJumps() {
    return implausibleJumps;
  }

  /** Entities evicted for idleness or to stay within the entity limit. */
  public long getEvictions() {
    return evictions;
  }

  /** Entities ended and started again because a report changed their domain or source. */
  public long getRestarts() {
    return restarts;
  }

  /** Entry or exit slots that used the nearest-neighbor fallback. */
  public long getNonAdjacentSlots() {
    return nonAdjacentSlots;
  }

  public LongUnaryOperator getBucketFunction() {
    return bucketFunction;
  }

  private MotionProfile profile(final MotionDomain domain, final MotionSource source) {
    final Map<MotionSource, MotionProfile> bySource =
        profiles.computeIfAbsent(domain, d -> new HashMap<>());
    MotionProfile profile = bySource.get(source);
    if (profile == null) {
      profile = new MotionProfile(domain, source, gapPolicy, resolutionPlan);
      bySource.put(source, profile);
      if (profile.levels.length > 0) {
        maxTimeoutMillis = Math.max(maxTimeoutMillis, profile.timeoutMillis);
        // reports are at most a window apart, so a crossing can find an excursion that much older
        for (int i = 0; i < profile.levels.length; i++) {
          levelHoldMillis[profile.levels[i]] =
              Math.max(
                  levelHoldMillis[profile.levels[i]],
                  saturatedAdd(profile.excursionMaxMillis[i], profile.windowMillis));
        }
      }
    }
    return profile;
  }

  /** Unstable entities emit no gap, so they are kept only while a reappearance could bridge. */
  private long ttl(final MotionEntity<K> entity) {
    if (entity.stable) {
      return idleTtlMillis;
    }
    return Math.min(
        idleTtlMillis,
        saturatedAdd(
            entity.profile.bridgedWindowMillis,
            entity.profile.source.getAllowedLatenessMillis()));
  }

  private void updateCompleteThrough() {
    // without a known source there is no back-off yet, and a watermark never moves back
    if ((processedTime != Long.MIN_VALUE) && (maxTimeoutMillis > 0)) {
      completeThrough =
          Math.max(completeThrough, saturatedSubtract(processedTime, maxTimeoutMillis));
      for (int level = 0; level < levelHoldMillis.length; level++) {
        levelCompleteThrough[level] =
            Math.max(
                levelCompleteThrough[level],
                saturatedSubtract(completeThrough, levelHoldMillis[level]));
      }
    }
  }

  private void makeRoom() {
    while (entities.size() >= maxEntities) {
      MotionEntity<K> victim = dormant.peek();
      if (victim == null) {
        victim = active.peek();
      }
      if (victim == null) {
        return;
      }
      evictions++;
      evict(victim);
    }
  }

  private void evict(final MotionEntity<K> entity) {
    if (!entity.inGap) {
      startGap(entity);
    }
    endGapCensored(entity);
    remove(entity);
  }

  private void remove(final MotionEntity<K> entity) {
    entities.remove(entity.key);
    if (entity.heap != null) {
      entity.heap.remove(entity);
    }
  }

  private void finish(final MotionEntity<K> entity, final boolean censored) {
    if (entity.inGap) {
      endGapCensored(entity);
    } else {
      endVisits(entity, entity.lastTime, censored);
    }
  }

  private long[] cellsAt(
      final MotionProfile profile,
      final double latitude,
      final double longitude) {
    final long[] cells = new long[profile.levels.length];
    cells[0] = h3.latLngToCell(latitude, longitude, profile.levels[0]);
    for (int i = 1; i < cells.length; i++) {
      cells[i] = h3.cellToParent(cells[0], profile.levels[i]);
    }
    return cells;
  }

  private void begin(
      final MotionEntity<K> entity,
      final MotionReport<K> report,
      final long[] cells) {
    final long time = report.getTime();
    entity.lastTime = time;
    entity.latitude = report.getLatitude();
    entity.longitude = report.getLongitude();
    entity.reportLatitude = entity.latitude;
    entity.reportLongitude = entity.longitude;
    entity.altitude = report.getAltitude();
    entity.inGap = false;
    // a stop that was already under way is not a stop start
    entity.stopped = report.hasSpeed() && (report.getSpeed() < gapPolicy.getStopEnterSpeed());
    entity.fastReports = 0;
    resetBaseline(entity, report);
    Arrays.fill(entity.excursions, null);
    System.arraycopy(cells, 0, entity.cells, 0, cells.length);
    for (int i = 0; i < cells.length; i++) {
      entity.visits[i].begin(
          cells[i],
          time,
          MotionSlice.ENDPOINT,
          entity.latitude,
          entity.longitude);
      visitStarted(entity, i);
    }
  }

  private static void resetBaseline(final MotionEntity<?> entity, final MotionReport<?> report) {
    entity.baseTime = report.getTime();
    entity.baseLatitude = report.getLatitude();
    entity.baseLongitude = report.getLongitude();
  }

  private void advance(
      final MotionEntity<K> entity,
      final MotionReport<K> report,
      final long elapsed) {
    final MotionProfile profile = entity.profile;
    final double from = entity.lastTime;
    final double to = report.getTime();
    if (entity.stopped) {
      if (report.hasSpeed() && (report.getSpeed() > gapPolicy.getStopLeaveSpeed())) {
        if (entity.fastReports == 0) {
          entity.fastSince = report.getTime();
        }
        entity.fastReports++;
      } else {
        entity.fastReports = 0;
      }
      final boolean fastEnough =
          (entity.fastReports >= gapPolicy.getStopLeaveReports())
              && ((report.getTime() - entity.fastSince) >= gapPolicy.getStopLeaveMinMillis());
      if (fastEnough
          || (Spherical.distance(
              entity.latitude,
              entity.longitude,
              report.getLatitude(),
              report.getLongitude()) > profile.stopRadiusMeters)) {
        entity.stopped = false;
        entity.fastReports = 0;
        if (entity.stable) {
          for (int i = 0; i < entity.cells.length; i++) {
            emit(slice(Type.START, entity, i, entity.cells[i], from, from, from));
          }
        }
        move(entity, from, to, report);
        resetBaseline(entity, report);
      } else {
        occupy(
            entity,
            entity.cells,
            from,
            to,
            true,
            0,
            altitude(entity.altitude, report.getAltitude(), 0.5),
            false);
      }
    } else {
      move(entity, from, to, report);
      if (slow(entity, report)) {
        stop(entity, to);
      }
    }
    entity.lastTime = report.getTime();
    entity.altitude = report.getAltitude();
    entity.reportLatitude = report.getLatitude();
    entity.reportLongitude = report.getLongitude();
    for (int i = 0; i < entity.excursions.length; i++) {
      final Excursion excursion = entity.excursions[i];
      if (excursion != null) {
        if (entity.cells[i] == excursion.cell) {
          Spherical.toVector(entity.latitude, entity.longitude, probe);
          excursion.deepen(probe);
        }
        if ((excursion.depthMeters > profile.excursionDepthMeters[i])
            || ((to - excursion.time) > profile.excursionMaxMillis[i])) {
          commit(entity, i);
        }
      }
    }
  }

  /**
   * Whether a moving entity has slowed below the stop speed. Without a reported speed the speed is
   * measured over a baseline, so position noise between close reports cannot read as a stop.
   */
  private boolean slow(final MotionEntity<K> entity, final MotionReport<K> report) {
    if (report.hasSpeed()) {
      resetBaseline(entity, report);
      return report.getSpeed() < gapPolicy.getStopEnterSpeed();
    }
    final long baseline = report.getTime() - entity.baseTime;
    if (baseline < entity.profile.speedlessBaselineMillis) {
      return false;
    }
    final double meters =
        Spherical.distance(
            entity.baseLatitude,
            entity.baseLongitude,
            report.getLatitude(),
            report.getLongitude());
    resetBaseline(entity, report);
    return (Math.max(0, meters - (2 * entity.profile.source.getPositionSigmaMeters()))
        / (baseline / 1000.0)) < gapPolicy.getStopEnterSpeed();
  }

  private void move(
      final MotionEntity<K> entity,
      final double from,
      final double to,
      final MotionReport<K> report) {
    final long endCell =
        h3.latLngToCell(report.getLatitude(), report.getLongitude(), entity.profile.levels[0]);
    walk(
        entity,
        entity.cells,
        from,
        to,
        report.getLatitude(),
        report.getLongitude(),
        report.getAltitude(),
        endCell,
        false);
    entity.latitude = report.getLatitude();
    entity.longitude = report.getLongitude();
  }

  /** Walks the chord from the entity's position to the given end, updating {@code cells}. */
  private void walk(
      final MotionEntity<K> entity,
      final long[] cells,
      final double from,
      final double to,
      final double latitude,
      final double longitude,
      final double endAltitude,
      final long endCell,
      final boolean bridged) {
    Spherical.toVector(entity.latitude, entity.longitude, start);
    Spherical.toVector(latitude, longitude, end);
    final double theta = Spherical.angle(start, end);
    final double meters = theta * Spherical.EARTH_RADIUS_METERS;
    findCrossings(theta, entity.profile, cells[0], endCell);
    final int count = crossingCount;
    final double duration = to - from;
    double s = 0;
    for (int k = 0; k < count; k++) {
      final double crossing = crossingS[k];
      final double time = from + (crossing * duration);
      occupy(
          entity,
          cells,
          from + (s * duration),
          time,
          false,
          meters * (crossing - s),
          altitude(entity.altitude, endAltitude, (s + crossing) / 2),
          bridged);
      Spherical.interpolate(start, end, theta, crossing, probe);
      cross(
          entity,
          cells,
          crossingFrom[k],
          crossingTo[k],
          time,
          Spherical.latitude(probe),
          Spherical.longitude(probe),
          bridged);
      s = crossing;
    }
    occupy(
        entity,
        cells,
        from + (s * duration),
        to,
        false,
        meters * (1 - s),
        altitude(entity.altitude, endAltitude, (s + 1) / 2),
        bridged);
  }

  private static double altitude(final double from, final double to, final double s) {
    if (Double.isNaN(from)) {
      return to;
    }
    if (Double.isNaN(to)) {
      return from;
    }
    return from + (s * (to - from));
  }

  private void findCrossings(
      final double theta,
      final MotionProfile profile,
      final long startCell,
      final long endCell) {
    crossingCount = 0;
    final int level = profile.levels[0];
    if ((startCell == endCell) && !((profile.distortedStepMeters > 0) && distorted(startCell))) {
      return;
    }
    final int samples =
        Math.max(1, (int) Math.ceil((theta * Spherical.EARTH_RADIUS_METERS) / profile.stepMeters));
    double previousS = 0;
    long previousCell = startCell;
    for (int i = 1; i <= samples; i++) {
      final double s = (double) i / samples;
      final long cell = i == samples ? endCell : cellAt(theta, s, level);
      scan(theta, level, profile.distortedStepMeters, previousS, previousCell, s, cell, 0);
      previousS = s;
      previousCell = cell;
    }
  }

  /**
   * Finds the crossings between two samples, bisecting cell changes and densifying in distorted
   * cells.
   */
  private void scan(
      final double theta,
      final int level,
      final double distortedStep,
      final double s0,
      final long cell0,
      final double s1,
      final long cell1,
      final int depth) {
    final double meters = (s1 - s0) * theta * Spherical.EARTH_RADIUS_METERS;
    if (cell0 == cell1) {
      if ((distortedStep > 0) && (meters > distortedStep) && distorted(cell0)) {
        final int pieces = (int) Math.ceil(meters / distortedStep);
        double previousS = s0;
        long previousCell = cell0;
        for (int j = 1; j <= pieces; j++) {
          final double s = j == pieces ? s1 : s0 + (((s1 - s0) * j) / pieces);
          final long cell = j == pieces ? cell1 : cellAt(theta, s, level);
          scan(theta, level, distortedStep, previousS, previousCell, s, cell, depth + 1);
          previousS = s;
          previousCell = cell;
        }
      }
      return;
    }
    final double middle = (s0 + s1) / 2;
    if ((meters <= CROSSING_PRECISION_METERS) || (depth >= MAX_BISECTIONS)) {
      addCrossing(middle, cell0, cell1);
      return;
    }
    final long middleCell = cellAt(theta, middle, level);
    scan(theta, level, distortedStep, s0, cell0, middle, middleCell, depth + 1);
    scan(theta, level, distortedStep, middle, middleCell, s1, cell1, depth + 1);
  }

  /** Whether a cell may not be convex: a pentagon, a pentagon's neighbor, or one on a face edge. */
  private boolean distorted(final long cell) {
    final int slot = (int) ((cell * 0x9E3779B97F4A7C15L) >>> (64 - DISTORTION_CACHE_BITS));
    if (distortionKeys[slot] == cell) {
      return distortionValues[slot];
    }
    boolean distorted = h3.isPentagon(cell) || (h3.cellToBoundary(cell).size() > 6);
    if (!distorted && h3.isPentagon(h3.cellToParent(cell, 0))) {
      for (final Long neighbor : h3.gridDisk(cell, 1)) {
        distorted |= h3.isPentagon(neighbor);
      }
    }
    distortionKeys[slot] = cell;
    distortionValues[slot] = distorted;
    return distorted;
  }

  private long cellAt(final double theta, final double s, final int level) {
    Spherical.interpolate(start, end, theta, s, probe);
    return h3.latLngToCell(Spherical.latitude(probe), Spherical.longitude(probe), level);
  }

  private void addCrossing(final double s, final long from, final long to) {
    if (crossingCount == crossingS.length) {
      final int length = crossingCount * 2;
      crossingS = Arrays.copyOf(crossingS, length);
      crossingFrom = Arrays.copyOf(crossingFrom, length);
      crossingTo = Arrays.copyOf(crossingTo, length);
    }
    crossingS[crossingCount] = s;
    crossingFrom[crossingCount] = from;
    crossingTo[crossingCount] = to;
    crossingCount++;
  }

  /** Accounts for time spent in {@code cells} from {@code from} to {@code to}. */
  private void occupy(
      final MotionEntity<K> entity,
      final long[] cells,
      final double from,
      final double to,
      final boolean stopped,
      final double meters,
      final double altitude,
      final boolean bridged) {
    if (!(to > from)) {
      return;
    }
    final double totalSeconds = (to - from) / 1000.0;
    if (!bridged) {
      final Totals totals = entity.excursions[0] != null ? entity.excursions[0] : entity.visits[0];
      totals.add(stopped ? 0 : totalSeconds, stopped ? totalSeconds : 0, meters, altitude);
    }
    double pieceStart = from;
    while (pieceStart < to) {
      final long startMillis = (long) Math.floor(pieceStart);
      final long bucket = bucketFunction.applyAsLong(startMillis);
      final double pieceEnd = Math.min(to, nextBucketStart(startMillis, bucket, to));
      final double seconds = (pieceEnd - pieceStart) / 1000.0;
      final double pieceMeters = (meters * seconds) / totalSeconds;
      for (int i = 0; i < cells.length; i++) {
        emit(
            new MotionSlice<>(
                Type.OCCUPANCY,
                entity.key,
                entity.profile.domain,
                entity.stable,
                entity.profile.levels[i],
                cells[i],
                bucket,
                (long) Math.floor(pieceStart),
                (long) Math.ceil(pieceEnd)).withOccupancy(
                    stopped ? 0 : seconds,
                    stopped ? seconds : 0,
                    pieceMeters,
                    bridged));
      }
      pieceStart = pieceEnd;
    }
  }

  /**
   * The first millisecond after {@code startMillis} in a different bucket, or infinity when the
   * piece ending at {@code to} stays in {@code bucket}. Any monotonic bucket function works.
   */
  private double nextBucketStart(final long startMillis, final long bucket, final double to) {
    final long last = (long) Math.ceil(to) - 1;
    if ((last <= startMillis) || (bucketFunction.applyAsLong(last) == bucket)) {
      return Double.POSITIVE_INFINITY;
    }
    long low = startMillis;
    long high = last;
    while ((high - low) > 1) {
      final long middle = low + ((high - low) / 2);
      if (bucketFunction.applyAsLong(middle) == bucket) {
        low = middle;
      } else {
        high = middle;
      }
    }
    return high;
  }

  private void cross(
      final MotionEntity<K> entity,
      final long[] cells,
      final long from,
      final long to,
      final double time,
      final double latitude,
      final double longitude,
      final boolean bridged) {
    final int[] levels = entity.profile.levels;
    cells[0] = to;
    for (int i = 1; i < cells.length; i++) {
      cells[i] = h3.cellToParent(to, levels[i]);
    }
    if (bridged) {
      return;
    }
    final Excursion excursion = entity.excursions[0];
    if (excursion == null) {
      startExcursion(entity, 0, from, to, time, latitude, longitude);
    } else if (returnsShallow(entity, 0, excursion, to, time)) {
      entity.visits[0].add(excursion);
      entity.excursions[0] = null;
    } else {
      commit(entity, 0);
      startExcursion(entity, 0, from, to, time, latitude, longitude);
    }
  }

  private static boolean returnsShallow(
      final MotionEntity<?> entity,
      final int index,
      final Excursion excursion,
      final long cell,
      final double time) {
    return (cell == excursion.fromCell)
        && (excursion.depthMeters <= entity.profile.excursionDepthMeters[index])
        && ((time - excursion.time) <= entity.profile.excursionMaxMillis[index]);
  }

  private void startExcursion(
      final MotionEntity<K> entity,
      final int index,
      final long from,
      final long to,
      final double time,
      final double latitude,
      final double longitude) {
    final Excursion excursion;
    if (h3.areNeighborCells(to, from)) {
      final long edge = h3.cellsToDirectedEdge(to, from);
      final List<LatLng> boundary = h3.directedEdgeToBoundary(edge);
      final double[][] vectors = new double[boundary.size()][3];
      for (int i = 0; i < vectors.length; i++) {
        Spherical.toVector(boundary.get(i).lat, boundary.get(i).lng, vectors[i]);
      }
      excursion = new Excursion(from, to, time, latitude, longitude, directionSlot(edge), vectors);
    } else {
      excursion = new Excursion(from, to, time, latitude, longitude, slot(to, from), null);
    }
    entity.excursions[index] = excursion;
    if (excursion.depthMeters > entity.profile.excursionDepthMeters[index]) {
      commit(entity, index);
    }
  }

  /** Makes a pending excursion a visit of its own. */
  private void commit(final MotionEntity<K> entity, final int index) {
    final Excursion excursion = entity.excursions[index];
    if (excursion == null) {
      return;
    }
    entity.excursions[index] = null;
    final Visit visit = entity.visits[index];
    if (index == 0) {
      for (int i = 1; i < entity.visits.length; i++) {
        coarseTotals(entity, i).add(visit);
      }
    }
    endVisit(
        entity,
        index,
        excursion.time,
        slot(excursion.fromCell, excursion.cell),
        excursion.latitude,
        excursion.longitude,
        false);
    visit.begin(
        excursion.cell,
        excursion.time,
        excursion.entrySlot,
        excursion.latitude,
        excursion.longitude);
    visit.add(excursion);
    visitStarted(entity, index);
    if (index == 0) {
      for (int i = 1; i < entity.visits.length; i++) {
        coarseStep(
            entity,
            i,
            h3.cellToParent(excursion.cell, entity.profile.levels[i]),
            excursion.time,
            excursion.latitude,
            excursion.longitude);
      }
    }
  }

  private static Totals coarseTotals(final MotionEntity<?> entity, final int index) {
    return entity.excursions[index] != null ? entity.excursions[index] : entity.visits[index];
  }

  /** Follows a resolved crossing-level visit change into the derived level {@code index}. */
  private void coarseStep(
      final MotionEntity<K> entity,
      final int index,
      final long parent,
      final double time,
      final double latitude,
      final double longitude) {
    final Excursion excursion = entity.excursions[index];
    final Visit visit = entity.visits[index];
    if (excursion == null) {
      if (parent != visit.cell) {
        startExcursion(entity, index, visit.cell, parent, time, latitude, longitude);
      }
    } else if (parent == excursion.cell) {
      Spherical.toVector(latitude, longitude, point);
      excursion.deepen(point);
      if (excursion.depthMeters > entity.profile.excursionDepthMeters[index]) {
        commit(entity, index);
      }
    } else if (returnsShallow(entity, index, excursion, parent, time)) {
      visit.add(excursion);
      entity.excursions[index] = null;
    } else {
      commit(entity, index);
      startExcursion(entity, index, visit.cell, parent, time, latitude, longitude);
    }
  }

  private void commitAll(final MotionEntity<K> entity) {
    for (int i = 0; i < entity.excursions.length; i++) {
      commit(entity, i);
    }
  }

  private void stop(final MotionEntity<K> entity, final double time) {
    commitAll(entity);
    entity.stopped = true;
    entity.fastReports = 0;
    for (int i = 0; i < entity.cells.length; i++) {
      if (entity.stable) {
        emit(slice(Type.STOP, entity, i, entity.cells[i], time, time, time));
      }
      passage(entity, i, time, MotionSlice.ENDPOINT);
    }
  }

  private void startGap(final MotionEntity<K> entity) {
    // committed, so the censored visit ends in the cell the gap starts from
    commitAll(entity);
    endVisits(entity, entity.lastTime, true);
    entity.inGap = true;
    entity.gapStart = entity.lastTime;
    entity.stopped = false;
    entity.fastReports = 0;
    if (entity.stable) {
      for (int i = 0; i < entity.cells.length; i++) {
        emit(
            slice(
                Type.GAP_START,
                entity,
                i,
                entity.cells[i],
                entity.gapStart,
                entity.gapStart,
                entity.gapStart));
      }
    }
  }

  private void reappear(final MotionEntity<K> entity, final MotionReport<K> report) {
    final long[] cells = cellsAt(entity.profile, report.getLatitude(), report.getLongitude());
    final long time = report.getTime();
    if (entity.stable) {
      for (int i = 0; i < cells.length; i++) {
        emit(
            slice(
                Type.GAP_END,
                entity,
                i,
                entity.cells[i],
                entity.gapStart,
                time,
                entity.gapStart).withGap(cells[i], false));
      }
    }
    final long gapMillis = time - entity.gapStart;
    final double jump =
        Spherical.distance(
            entity.reportLatitude,
            entity.reportLongitude,
            report.getLatitude(),
            report.getLongitude());
    if ((gapMillis <= entity.profile.bridgedWindowMillis)
        && gapPolicy.isPlausible(entity.profile.domain, entity.profile.source, jump, gapMillis)) {
      walk(
          entity,
          entity.cells.clone(),
          entity.gapStart,
          time,
          report.getLatitude(),
          report.getLongitude(),
          report.getAltitude(),
          cells[0],
          true);
    }
    begin(entity, report, cells);
  }

  private void endGapCensored(final MotionEntity<K> entity) {
    if (entity.stable) {
      final long end = Math.max(processedTime, entity.gapStart);
      for (int i = 0; i < entity.cells.length; i++) {
        emit(
            slice(
                Type.GAP_END,
                entity,
                i,
                entity.cells[i],
                entity.gapStart,
                end,
                entity.gapStart).withGap(0, true));
      }
    }
  }

  private void endVisits(final MotionEntity<K> entity, final double time, final boolean censored) {
    // at a track's end a pending excursion never went deep, so its time stays with its visit
    for (int i = 0; i < entity.excursions.length; i++) {
      if (entity.excursions[i] != null) {
        entity.visits[i].add(entity.excursions[i]);
        entity.excursions[i] = null;
      }
    }
    final Visit fine = entity.visits[0];
    for (int i = 1; i < entity.visits.length; i++) {
      entity.visits[i].add(fine);
    }
    for (int i = 0; i < entity.visits.length; i++) {
      endVisit(entity, i, time, MotionSlice.ENDPOINT, entity.latitude, entity.longitude, censored);
    }
  }

  private void endVisit(
      final MotionEntity<K> entity,
      final int index,
      final double time,
      final int exitSlot,
      final double latitude,
      final double longitude,
      final boolean censored) {
    final Visit visit = entity.visits[index];
    if (entity.stable) {
      final double speed =
          visit.movingSeconds >= 1 ? visit.distanceMeters / visit.movingSeconds : Double.NaN;
      final double displacement =
          Spherical.distance(visit.entryLatitude, visit.entryLongitude, latitude, longitude);
      final double heading =
          (displacement > 0) && (displacement >= entity.profile.headingMinMeters)
              ? Spherical.bearing(visit.entryLatitude, visit.entryLongitude, latitude, longitude)
              : Double.NaN;
      final double meanAltitude =
          visit.altitudeSeconds > 0 ? visit.altitudeSum / visit.altitudeSeconds : Double.NaN;
      final double traversal =
          (visit.passageEntrySlot < MotionSlice.ENDPOINT) && (exitSlot < MotionSlice.ENDPOINT)
              ? (time - visit.startTime) / 1000.0
              : Double.NaN;
      emit(
          slice(Type.VISIT_END, entity, index, visit.cell, visit.startTime, time, time).withSlots(
              visit.entrySlot,
              exitSlot).withVisit(
                  visit.movingSeconds,
                  visit.stoppedSeconds,
                  visit.distanceMeters,
                  speed,
                  heading,
                  meanAltitude,
                  traversal,
                  censored));
    }
    passage(entity, index, time, exitSlot);
  }

  private void passage(
      final MotionEntity<K> entity,
      final int index,
      final double time,
      final int exitSlot) {
    final Visit visit = entity.visits[index];
    if (entity.stable
        || ((visit.passageEntrySlot < MotionSlice.ENDPOINT) && (exitSlot < MotionSlice.ENDPOINT))) {
      emit(
          slice(Type.TRANSITION, entity, index, visit.cell, time, time, time).withSlots(
              visit.passageEntrySlot,
              exitSlot));
    }
    visit.passageEntrySlot = MotionSlice.ENDPOINT;
  }

  private void visitStarted(final MotionEntity<K> entity, final int index) {
    if (entity.stable) {
      final Visit visit = entity.visits[index];
      emit(
          slice(
              Type.VISIT_START,
              entity,
              index,
              visit.cell,
              visit.startTime,
              visit.startTime,
              visit.startTime).withSlots(visit.entrySlot, MotionSlice.NO_SLOT));
    }
  }

  /** The slot of the neighbor of {@code origin} toward {@code destination}. */
  private int slot(final long origin, final long destination) {
    if (h3.areNeighborCells(origin, destination)) {
      return directionSlot(h3.cellsToDirectedEdge(origin, destination));
    }
    nonAdjacentSlots++;
    final LatLng target = h3.cellToLatLng(destination);
    long nearestEdge = 0;
    double nearestMeters = Double.POSITIVE_INFINITY;
    for (final Long edge : h3.originToDirectedEdges(origin)) {
      final LatLng center = h3.cellToLatLng(h3.getDirectedEdgeDestination(edge));
      final double meters = Spherical.distance(center.lat, center.lng, target.lat, target.lng);
      if (meters < nearestMeters) {
        nearestMeters = meters;
        nearestEdge = edge;
      }
    }
    return directionSlot(nearestEdge);
  }

  /** H3 keeps a directed edge's direction digit (1 to 6) in bits 56 to 58. */
  private static int directionSlot(final long edge) {
    return (int) ((edge >>> 56) & 7) - 1;
  }

  private MotionSlice<K> slice(
      final Type type,
      final MotionEntity<K> entity,
      final int index,
      final long cell,
      final double startTime,
      final double endTime,
      final double attributedTime) {
    return new MotionSlice<>(
        type,
        entity.key,
        entity.profile.domain,
        entity.stable,
        entity.profile.levels[index],
        cell,
        bucketFunction.applyAsLong((long) Math.floor(attributedTime)),
        (long) Math.floor(startTime),
        (long) Math.floor(endTime));
  }

  private void emit(final MotionSlice<K> slice) {
    consumer.accept(slice);
  }

  public static final class Builder<K> {
    private final Consumer<? super MotionSlice<K>> consumer;
    private GapPolicy gapPolicy = GapPolicy.defaults();
    private ResolutionPlan resolutionPlan = ResolutionPlan.defaults();
    private LongUnaryOperator bucketFunction = HOUR;
    private int maxEntities = 1_000_000;
    private long idleTtlMillis = 12 * 3_600_000L;
    private final List<ExpectedSource> expectedSources = new ArrayList<>();

    private Builder(final Consumer<? super MotionSlice<K>> consumer) {
      this.consumer = Objects.requireNonNull(consumer, "consumer");
    }

    public Builder<K> gapPolicy(final GapPolicy policy) {
      gapPolicy = Objects.requireNonNull(policy, "policy");
      return this;
    }

    public Builder<K> resolutionPlan(final ResolutionPlan plan) {
      resolutionPlan = Objects.requireNonNull(plan, "plan");
      return this;
    }

    /**
     * Maps an epoch millisecond to the start of its bucket. It must be monotonic. Defaults to
     * {@link H3MotionSegmenter#HOUR}.
     */
    public Builder<K> bucketFunction(final LongUnaryOperator function) {
      bucketFunction = Objects.requireNonNull(function, "function");
      return this;
    }

    /** The most entities held at once; defaults to a million. */
    public Builder<K> maxEntities(final int max) {
      if (max < 1) {
        throw new IllegalArgumentException("At least one entity must fit");
      }
      maxEntities = max;
      return this;
    }

    /**
     * How long a stable entity in a gap is kept so that its gap can still be measured when it
     * reappears; defaults to 12 hours. An unstable entity is kept for at most its domain's bridged
     * window plus its source's lateness. An entity is never evicted for idleness before it has
     * timed out.
     */
    public Builder<K> idleTtlMillis(final long millis) {
      if (millis < 0) {
        throw new IllegalArgumentException("The idle TTL must be non-negative");
      }
      idleTtlMillis = millis;
      return this;
    }

    /**
     * Declares a source whose reports are expected on {@code domain}, so that the watermarks back
     * off by its timeout and level holds from the start instead of from its first report. A source
     * first seen after the watermarks have advanced cannot move them back, so its first slices can
     * land behind them.
     */
    public Builder<K> expectSource(final MotionDomain domain, final MotionSource source) {
      expectedSources.add(
          new ExpectedSource(
              Objects.requireNonNull(domain, "domain"),
              Objects.requireNonNull(source, "source")));
      return this;
    }

    public H3MotionSegmenter<K> build() {
      return new H3MotionSegmenter<>(this);
    }
  }

  private static final class ExpectedSource {
    private final MotionDomain domain;
    private final MotionSource source;

    private ExpectedSource(final MotionDomain domain, final MotionSource source) {
      this.domain = domain;
      this.source = source;
    }
  }
}
