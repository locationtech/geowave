/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.binning.ComplexGeometryBinningOption;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.store.statistics.binning.SpatialFieldValueBinningStrategy;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

/**
 * Regression coverage for {@code DataStore.incorporateStatisticFromEntry} against a binning
 * strategy whose weighting is bin-dependent.
 *
 * <p> The failure mode this guards against is computing a single
 * {@link org.locationtech.geowave.core.store.api.StatisticValue} with no bin set and then
 * incorporating that same instance into every bin.
 * {@link SpatialFieldValueBinningStrategy#getWeight} starts from a weight of 1 and refines it by
 * walking the bin's bytes, so an unset bin leaves the loop body unexecuted and yields a weight of 1
 * for every bin. A geometry spanning N cells then contributes a full count to each of them rather
 * than its share, inflating the total N-fold.
 */
public class IncorporateStatisticFromEntryTest {

  private static final String TYPE_NAME = "Region";
  private static final int ENTRY_COUNT = 200;
  private static final int S2_PRECISION = 6;

  /** Large enough to span several S2 cells at the precision under test. */
  private static final Geometry REGION =
      GeometryUtils.GEOMETRY_FACTORY.createPolygon(
          new Coordinate[] {
              new Coordinate(10, 10),
              new Coordinate(14, 10),
              new Coordinate(14, 14),
              new Coordinate(10, 14),
              new Coordinate(10, 10)});

  public static class Region {
    private String id;
    private Geometry geometry;

    public Region() {}

    public Region(final String id, final Geometry geometry) {
      this.id = id;
      this.geometry = geometry;
    }

    public String getId() {
      return id;
    }

    public void setId(final String id) {
      this.id = id;
    }

    public Geometry getGeometry() {
      return geometry;
    }

    public void setGeometry(final Geometry geometry) {
      this.geometry = geometry;
    }
  }

  private DataStore dataStore;

  @Before
  public void createStore() {
    dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());
    dataStore.addType(
        BasicDataTypeAdapter.newAdapter(TYPE_NAME, Region.class, "id"),
        new SpatialIndexBuilder().createIndex());
  }

  @Test
  public void weightsAreComputedPerBinRatherThanOncePerEntry() {
    final CountStatistic count = new CountStatistic(TYPE_NAME);
    count.setTag("overlap-scaled");
    count.setBinningStrategy(
        new SpatialFieldValueBinningStrategy(
            SpatialBinningType.S2,
            S2_PRECISION,
            ComplexGeometryBinningOption.USE_FULL_GEOMETRY_SCALE_BY_OVERLAP,
            "geometry"));
    dataStore.addEmptyStatistic(count);

    for (int i = 0; i < ENTRY_COUNT; i++) {
      dataStore.incorporateStatisticFromEntry(count, TYPE_NAME, new Region("r" + i, REGION));
    }

    long bins = 0;
    long total = 0;
    try (CloseableIterator<Pair<ByteArray, Long>> it = dataStore.getBinnedStatisticValues(count)) {
      while (it.hasNext()) {
        bins++;
        total += it.next().getValue();
      }
    }

    assertTrue(
        "the fixture geometry must span more than one bin for this test to mean anything, spanned "
            + bins,
        bins > 1);
    // Each entry's weight is split across the bins it overlaps, so the per-bin counts sum back to
    // the number of entries. Reusing one unbinned value would instead give ENTRY_COUNT per bin.
    assertEquals(
        "per-bin counts should sum to the entry count, not to entries * bins",
        ENTRY_COUNT,
        total,
        ENTRY_COUNT * 0.05);
  }

  @Test
  public void unbinnedStatisticsStillAccumulate() {
    final CountStatistic count = new CountStatistic(TYPE_NAME);
    count.setTag("unbinned");
    dataStore.addEmptyStatistic(count);

    for (int i = 0; i < ENTRY_COUNT; i++) {
      dataStore.incorporateStatisticFromEntry(count, TYPE_NAME, new Region("r" + i, REGION));
    }

    assertEquals(Long.valueOf(ENTRY_COUNT), dataStore.getStatisticValue(count));
  }
}
