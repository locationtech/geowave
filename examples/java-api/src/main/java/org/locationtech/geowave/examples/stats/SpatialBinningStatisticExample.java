/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples.stats;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.statistics.binning.SpatialFieldValueBinningStrategy;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericStatsStatistic;
import org.locationtech.geowave.core.store.statistics.field.Stats;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import com.google.common.collect.ImmutableList;

public class SpatialBinningStatisticExample {
  public static void main(final String[] args) {
    final SimpleFeatureType featureType = getSimpleFeatureType();
    // Points (to be ingested into GeoWave Data Store)
    final List<SimpleFeature> cannedFeatures =
        ImmutableList.of(
            buildSimpleFeature(featureType, "Loc1", new Coordinate(-77.0352, 38.8895), 12),
            buildSimpleFeature(featureType, "Loc2", new Coordinate(-77.0366, 38.8977), 13),
            buildSimpleFeature(featureType, "Loc3", new Coordinate(-76.8644, 38.9078), 8),
            buildSimpleFeature(featureType, "Loc4", new Coordinate(-76.350677, 38.9641511), 15),
            buildSimpleFeature(featureType, "Loc5", new Coordinate(-77.3384112, 38.416091), 7),
            buildSimpleFeature(featureType, "Loc6", new Coordinate(-67.0352, 28.8895), 3),
            buildSimpleFeature(featureType, "Loc7", new Coordinate(-67.0366, 28.8977), 99),
            buildSimpleFeature(featureType, "Loc8", new Coordinate(-66.8644, 28.9078), 0),
            buildSimpleFeature(featureType, "Loc9", new Coordinate(-66.350677, 28.9641511), 1),
            buildSimpleFeature(featureType, "Loc10", new Coordinate(-67.3384112, 28.416091), 23));

    final Index index =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    final DataStore dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());
    final FeatureDataAdapter adapter = new FeatureDataAdapter(featureType);
    final Envelope bbox1 = new Envelope(-77.5, -76, 38.4, 39);
    final Envelope bbox2 = new Envelope(-67.5, -66, 28.4, 29);

    dataStore.addType(adapter, index);
    final CountStatistic s2Count = new CountStatistic(featureType.getTypeName());
    s2Count.setTag("S2-Example");
    final SpatialFieldValueBinningStrategy s2SpatialBinning =
        new SpatialFieldValueBinningStrategy(featureType.getGeometryDescriptor().getLocalName());
    // type could be Google's S2, Uber's H3, or simple GeoHash
    s2SpatialBinning.setType(SpatialBinningType.S2);
    // precision is the character length for H3 and GeoHash which is over twice as coarse as S2
    // which uses powers of two for precision (so a precision of 8 in S2 is actually a coarser
    // granularity than a precision of 4 in GeoHash or H3)
    s2SpatialBinning.setPrecision(7);
    s2Count.setBinningStrategy(s2SpatialBinning);

    final CountStatistic h3Count = new CountStatistic(featureType.getTypeName());
    // stats for the same feature type should have different tags
    h3Count.setTag("H3-Example");
    final SpatialFieldValueBinningStrategy h3SpatialBinning =
        new SpatialFieldValueBinningStrategy(featureType.getGeometryDescriptor().getLocalName());
    // type could be Google's S2, Uber's H3, or simple GeoHash
    h3SpatialBinning.setType(SpatialBinningType.H3);
    h3SpatialBinning.setPrecision(3);
    h3Count.setBinningStrategy(h3SpatialBinning);

    final CountStatistic geohashCount = new CountStatistic(featureType.getTypeName());
    geohashCount.setTag("Geohash-Example");
    final SpatialFieldValueBinningStrategy geohashSpatialBinning =
        new SpatialFieldValueBinningStrategy(featureType.getGeometryDescriptor().getLocalName());
    // type could be Google's S2, Uber's H3, or simple GeoHash
    geohashSpatialBinning.setType(SpatialBinningType.GEOHASH);
    geohashSpatialBinning.setPrecision(3);
    geohashCount.setBinningStrategy(geohashSpatialBinning);

    // you can add "empty" statistic before you've written any data, the stats will then be updated
    // as you write data

    // alternatively if you don't use the "empty" variant it will automatically calculate and update
    // these stats for pre-existing data before returning from the method
    dataStore.addEmptyStatistic(s2Count, h3Count, geohashCount);

    // Ingest cannedFeatures into the DataStore.
    try (Writer<SimpleFeature> indexWriter = dataStore.createWriter(adapter.getTypeName())) {
      for (final SimpleFeature sf : cannedFeatures) {
        indexWriter.write(sf);
      }
    }
    System.out.println("***** S2 Binning *****");
    System.out.println("** All Bins **");
    try (
        CloseableIterator<Pair<ByteArray, Long>> it = dataStore.getBinnedStatisticValues(s2Count)) {
      // you can get all bins
      while (it.hasNext()) {
        final Pair<ByteArray, Long> pair = it.next();
        System.out.println(
            String.format(
                "Count: %d, Bin: %s, Bin Geometry: %s",
                pair.getRight(),
                s2SpatialBinning.binToString(pair.getLeft()),
                s2SpatialBinning.getType().getBinGeometry(pair.getLeft(), 7)));
      }
    }
    System.out.println(String.format("** Bins Within Envelope %s **", bbox1));
    try (CloseableIterator<Pair<ByteArray, Long>> it =
        dataStore.getBinnedStatisticValues(s2Count, BinConstraints.ofObject(bbox1))) {
      // or you can get only bins within an envelope
      while (it.hasNext()) {
        final Pair<ByteArray, Long> pair = it.next();
        System.out.println(
            String.format(
                "Count: %d, Bin: %s, Bin Geometry: %s",
                pair.getRight(),
                s2SpatialBinning.binToString(pair.getLeft()),
                s2SpatialBinning.getType().getBinGeometry(pair.getLeft(), 7)));
      }
    }

    // or you could just get the aggregated statistic value for an envelope (keep in mind this is
    // using the statistic bins that intersect the envelope so may be an over-estimate for bins that
    // only partially intersect)
    System.out.println(
        String.format(
            "** %d in bbox %s **",
            dataStore.getStatisticValue(s2Count, BinConstraints.ofObject(bbox2)),
            bbox2));

    System.out.println("\n***** H3 Binning *****");
    System.out.println("** All Bins **");
    try (
        CloseableIterator<Pair<ByteArray, Long>> it = dataStore.getBinnedStatisticValues(h3Count)) {
      // you can get all bins
      while (it.hasNext()) {
        final Pair<ByteArray, Long> pair = it.next();
        System.out.println(
            String.format(
                "Count: %d, Bin: %s, Bin Geometry: %s",
                pair.getRight(),
                h3SpatialBinning.binToString(pair.getLeft()),
                h3SpatialBinning.getType().getBinGeometry(pair.getLeft(), 3)));
      }
    }
    System.out.println(String.format("** Bins Within Envelope %s **", bbox1));
    try (CloseableIterator<Pair<ByteArray, Long>> it =
        dataStore.getBinnedStatisticValues(h3Count, BinConstraints.ofObject(bbox1))) {
      // or you can get only bins within an envelope
      while (it.hasNext()) {
        final Pair<ByteArray, Long> pair = it.next();
        System.out.println(
            String.format(
                "Count: %d, Bin: %s, Bin Geometry: %s",
                pair.getRight(),
                h3SpatialBinning.binToString(pair.getLeft()),
                h3SpatialBinning.getType().getBinGeometry(pair.getLeft(), 3)));
      }
    }

    // or you could just get the aggregated statistic value for an envelope (keep in mind this is
    // using the statistic bins that intersect the envelope so may be an over-estimate for bins that
    // only partially intersect)
    System.out.println(
        String.format(
            "** %d in bbox %s **",
            dataStore.getStatisticValue(h3Count, BinConstraints.ofObject(bbox2)),
            bbox2));

    System.out.println("\n***** Geohash Binning *****");
    System.out.println("** All Bins **");
    try (CloseableIterator<Pair<ByteArray, Long>> it =
        dataStore.getBinnedStatisticValues(geohashCount)) {
      // you can get all bins
      while (it.hasNext()) {
        final Pair<ByteArray, Long> pair = it.next();
        System.out.println(
            String.format(
                "Count: %d, Bin: %s, Bin Geometry: %s",
                pair.getRight(),
                geohashSpatialBinning.binToString(pair.getLeft()),
                geohashSpatialBinning.getType().getBinGeometry(pair.getLeft(), 3)));
      }
    }
    System.out.println(String.format("** Bins Within Envelope %s **", bbox1));
    try (CloseableIterator<Pair<ByteArray, Long>> it =
        dataStore.getBinnedStatisticValues(geohashCount, BinConstraints.ofObject(bbox1))) {
      // or you can get only bins within an envelope
      while (it.hasNext()) {
        final Pair<ByteArray, Long> pair = it.next();
        System.out.println(
            String.format(
                "Count: %d, Bin: %s, Bin Geometry: %s",
                pair.getRight(),
                geohashSpatialBinning.binToString(pair.getLeft()),
                geohashSpatialBinning.getType().getBinGeometry(pair.getLeft(), 3)));
      }
    }

    // or you could just get the aggregated statistic value for an envelope (keep in mind this is
    // using the statistic bins that intersect the envelope so may be an over-estimate for bins that
    // only partially intersect)
    System.out.println(
        String.format(
            "** %d in bbox %s **",
            dataStore.getStatisticValue(geohashCount, BinConstraints.ofObject(bbox2)),
            bbox2));

    // and finally just to make it clear, you can apply spatial binning to *any* statistic not just
    // counts

    // so here's an example binning numeric stats of the population (sum, avg, std dev, etc.) by an
    // S2 level 7 grid
    final NumericStatsStatistic s2PopulationStats =
        new NumericStatsStatistic(featureType.getTypeName(), "population");
    s2PopulationStats.setTag("S2-Population-Stats");
    final SpatialFieldValueBinningStrategy s2PopulationSpatialBinning =
        new SpatialFieldValueBinningStrategy(featureType.getGeometryDescriptor().getLocalName());
    s2PopulationSpatialBinning.setType(SpatialBinningType.S2);
    s2PopulationSpatialBinning.setPrecision(7);
    s2PopulationStats.setBinningStrategy(s2PopulationSpatialBinning);
    // here we'll calculate the stat on add based on the already written data (rather than adding
    // the "empty" statistic)
    dataStore.addStatistic(s2PopulationStats);
    // and we'll run through the same set of examples of getting all the bins and then filtering by
    // an envelope
    System.out.println("\n***** S2 Population Stats Binning *****");
    System.out.println("** All Bins **");
    try (CloseableIterator<Pair<ByteArray, Stats>> it =
        dataStore.getBinnedStatisticValues(s2PopulationStats)) {
      // you can get all bins
      while (it.hasNext()) {
        final Pair<ByteArray, Stats> pair = it.next();
        System.out.println(
            String.format(
                "Population: %s, Bin: %s, Bin Geometry: %s",
                pair.getRight(),
                s2PopulationSpatialBinning.binToString(pair.getLeft()),
                s2PopulationSpatialBinning.getType().getBinGeometry(pair.getLeft(), 3)));
      }
    }
    System.out.println(String.format("** Bins Within Envelope %s **", bbox1));
    try (CloseableIterator<Pair<ByteArray, Stats>> it =
        dataStore.getBinnedStatisticValues(s2PopulationStats, BinConstraints.ofObject(bbox1))) {
      // or you can get only bins within an envelope
      while (it.hasNext()) {
        final Pair<ByteArray, Stats> pair = it.next();
        System.out.println(
            String.format(
                "Population: %s, Bin: %s, Bin Geometry: %s",
                pair.getRight(),
                s2PopulationSpatialBinning.binToString(pair.getLeft()),
                s2PopulationSpatialBinning.getType().getBinGeometry(pair.getLeft(), 3)));
      }
    }
    // or you could just get the aggregated statistic value for an envelope (keep in mind this is
    // using the statistic bins that intersect the envelope so may be an over-estimate for bins that
    // only partially intersect)
    System.out.println(
        String.format(
            "** Population Stats '%s' in bbox %s **",
            dataStore.getStatisticValue(s2PopulationStats, BinConstraints.ofObject(bbox2)),
            bbox2));

  }

  /**
   * A helper that constructs the SimpleFeatureType used in this example.
   */
  private static SimpleFeatureType getSimpleFeatureType() {
    final String name = "ExampleSimpleFeatureType";
    final SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
    final AttributeTypeBuilder atBuilder = new AttributeTypeBuilder();
    sftBuilder.setName(name);
    // the location name isn't used in this example, its just here for show!
    sftBuilder.add(atBuilder.binding(String.class).nillable(false).buildDescriptor("locationName"));
    // this is used for the grouping (the `.bin` call).
    sftBuilder.add(atBuilder.binding(Geometry.class).nillable(false).buildDescriptor("geometry"));
    // this is the field that is summed in each group, as defined by the `.aggregate` call.
    sftBuilder.add(atBuilder.binding(Integer.class).nillable(false).buildDescriptor("population"));

    return sftBuilder.buildFeatureType();
  }

  /**
   * Just a helper method to create a SimpleFeature to the specifications used in this example.
   */
  private static SimpleFeature buildSimpleFeature(
      final SimpleFeatureType featureType,
      final String locationName,
      final Coordinate coordinate,
      final int population) {
    final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);
    builder.set("locationName", locationName);
    builder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate));
    builder.set("population", population);

    return builder.buildFeature(locationName);
  }
}

