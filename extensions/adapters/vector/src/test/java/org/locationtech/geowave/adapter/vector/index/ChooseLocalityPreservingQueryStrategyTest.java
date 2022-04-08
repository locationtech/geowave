/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.index.api.SpatialTemporalIndexBuilder;
import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass.ConstraintData;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass.ConstraintSet;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass.ConstraintsByClass;
import com.beust.jcommander.internal.Maps;

public class ChooseLocalityPreservingQueryStrategyTest {
  private static final double HOUR = 3600000;
  private static final double DAY = HOUR * 24;
  private static final double WEEK = DAY * 7;
  private static final double HOUSE = 0.005;
  private static final double BLOCK = 0.07;
  private static final double CITY = 1.25;
  final Index IMAGE_CHIP_INDEX1 = new NullIndex("IMAGERY_CHIPS1");
  final Index IMAGE_CHIP_INDEX2 = new NullIndex("IMAGERY_CHIPS2");

  protected final List<Index> indices =
      Arrays.asList(
          IMAGE_CHIP_INDEX1,
          new SpatialTemporalIndexBuilder().setNumPartitions(5).setBias(
              SpatialTemporalDimensionalityTypeProvider.Bias.BALANCED).setPeriodicity(
                  Unit.YEAR).createIndex(),
          new SpatialTemporalIndexBuilder().setNumPartitions(10).setBias(
              SpatialTemporalDimensionalityTypeProvider.Bias.BALANCED).setPeriodicity(
                  Unit.DAY).createIndex(),
          new SpatialIndexBuilder().createIndex(),
          IMAGE_CHIP_INDEX2);

  @Test
  public void testChooseTemporalWithoutStatsHouseHour() {
    final ChooseLocalityPreservingQueryStrategy strategy =
        new ChooseLocalityPreservingQueryStrategy();

    final Iterator<Index> it =
        getIndices(new BasicQueryByClass(createConstraints(HOUSE, HOUSE, HOUR)), strategy);
    assertTrue(it.hasNext());
    assertEquals(indices.get(1).getName(), it.next().getName());
    assertFalse(it.hasNext());
  }

  @Test
  public void testChooseSpatialWithoutStatsHouseDay() {
    final ChooseLocalityPreservingQueryStrategy strategy =
        new ChooseLocalityPreservingQueryStrategy();

    final Iterator<Index> it =
        getIndices(new BasicQueryByClass(createConstraints(HOUSE, HOUSE, DAY)), strategy);
    assertTrue(it.hasNext());
    assertEquals(indices.get(3).getName(), it.next().getName());
    assertFalse(it.hasNext());
  }

  @Test
  public void testChooseSpatialWithoutStatsHouseWeek() {
    final ChooseLocalityPreservingQueryStrategy strategy =
        new ChooseLocalityPreservingQueryStrategy();

    final Iterator<Index> it =
        getIndices(new BasicQueryByClass(createConstraints(HOUSE, HOUSE, WEEK)), strategy);
    assertTrue(it.hasNext());
    assertEquals(indices.get(3).getName(), it.next().getName());
    assertFalse(it.hasNext());
  }

  @Test
  public void testChooseTemporalWithoutStatsBlockHour() {
    final ChooseLocalityPreservingQueryStrategy strategy =
        new ChooseLocalityPreservingQueryStrategy();

    final Iterator<Index> it =
        getIndices(new BasicQueryByClass(createConstraints(BLOCK, BLOCK, HOUR)), strategy);
    assertTrue(it.hasNext());
    assertEquals(indices.get(1).getName(), it.next().getName());
    assertFalse(it.hasNext());
  }

  @Test
  public void testChooseTemporalWithoutStatsBlockDay() {
    final ChooseLocalityPreservingQueryStrategy strategy =
        new ChooseLocalityPreservingQueryStrategy();

    final Iterator<Index> it =
        getIndices(new BasicQueryByClass(createConstraints(BLOCK, BLOCK, DAY)), strategy);
    assertTrue(it.hasNext());
    assertEquals(indices.get(1).getName(), it.next().getName());
    assertFalse(it.hasNext());
  }

  @Test
  public void testChooseSpatialWithoutStatsBlockWeek() {
    final ChooseLocalityPreservingQueryStrategy strategy =
        new ChooseLocalityPreservingQueryStrategy();

    final Iterator<Index> it =
        getIndices(new BasicQueryByClass(createConstraints(BLOCK, BLOCK, WEEK)), strategy);
    assertTrue(it.hasNext());
    assertEquals(indices.get(3).getName(), it.next().getName());
    assertFalse(it.hasNext());
  }

  @Test
  public void testChooseTemporalWithoutStatsCityHour() {
    final ChooseLocalityPreservingQueryStrategy strategy =
        new ChooseLocalityPreservingQueryStrategy();

    final Iterator<Index> it =
        getIndices(new BasicQueryByClass(createConstraints(CITY, CITY, HOUR)), strategy);
    assertTrue(it.hasNext());
    assertEquals(indices.get(1).getName(), it.next().getName());
    assertFalse(it.hasNext());
  }

  @Test
  public void testChooseTemporalWithoutStatsCityDay() {
    final ChooseLocalityPreservingQueryStrategy strategy =
        new ChooseLocalityPreservingQueryStrategy();

    final Iterator<Index> it =
        getIndices(new BasicQueryByClass(createConstraints(CITY, CITY, DAY)), strategy);
    assertTrue(it.hasNext());
    assertEquals(indices.get(1).getName(), it.next().getName());
    assertFalse(it.hasNext());
  }

  @Test
  public void testChooseTemporalWithoutStatsCityWeek() {
    final ChooseLocalityPreservingQueryStrategy strategy =
        new ChooseLocalityPreservingQueryStrategy();

    final Iterator<Index> it =
        getIndices(new BasicQueryByClass(createConstraints(CITY, CITY, WEEK)), strategy);
    assertTrue(it.hasNext());
    assertEquals(indices.get(1).getName(), it.next().getName());
    assertFalse(it.hasNext());
  }

  public Iterator<Index> getIndices(
      final BasicQueryByClass query,
      final ChooseLocalityPreservingQueryStrategy strategy) {
    return strategy.getIndices(
        null,
        null,
        query,
        indices.toArray(new Index[indices.size()]),
        null,
        Maps.newHashMap());
  }

  public static class ConstrainedIndexValue extends NumericRange {

    /** */
    private static final long serialVersionUID = 1L;

    public ConstrainedIndexValue(final double min, final double max) {
      super(min, max);
      //
    }

  }

  private ConstraintsByClass createConstraints(
      final double lat,
      final double lon,
      final double time) {
    final ConstraintSet cs1 = new ConstraintSet();
    cs1.addConstraint(
        LatitudeDefinition.class,
        new ConstraintData(new ConstrainedIndexValue(0, lat), true));

    cs1.addConstraint(
        LongitudeDefinition.class,
        new ConstraintData(new ConstrainedIndexValue(0, lon), true));

    final ConstraintSet cs2a = new ConstraintSet();
    cs2a.addConstraint(
        TimeDefinition.class,
        new ConstraintData(new ConstrainedIndexValue(0, time), true));

    return new ConstraintsByClass(Arrays.asList(cs2a)).merge(Collections.singletonList(cs1));
  }
}
