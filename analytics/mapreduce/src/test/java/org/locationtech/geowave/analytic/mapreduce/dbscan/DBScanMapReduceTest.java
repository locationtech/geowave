/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.dbscan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.FeatureWritable;
import org.locationtech.geowave.analytic.AdapterWithObjectWritable;
import org.locationtech.geowave.analytic.AnalyticFeature;
import org.locationtech.geowave.analytic.Projection;
import org.locationtech.geowave.analytic.SimpleFeatureProjection;
import org.locationtech.geowave.analytic.clustering.ClusteringUtils;
import org.locationtech.geowave.analytic.mapreduce.kmeans.SimpleFeatureImplSerialization;
import org.locationtech.geowave.analytic.mapreduce.nn.NNMapReduce;
import org.locationtech.geowave.analytic.mapreduce.nn.NNMapReduce.PartitionDataWritable;
import org.locationtech.geowave.analytic.param.ClusteringParameters;
import org.locationtech.geowave.analytic.param.HullParameters;
import org.locationtech.geowave.analytic.param.PartitionParameters;
import org.locationtech.geowave.analytic.partitioner.Partitioner.PartitionData;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.mapreduce.GeoWaveConfiguratorBase;
import org.locationtech.geowave.mapreduce.JobContextAdapterStore;
import org.locationtech.geowave.mapreduce.JobContextInternalAdapterStore;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class DBScanMapReduceTest {

  MapDriver<GeoWaveInputKey, Object, PartitionDataWritable, AdapterWithObjectWritable> mapDriver;
  ReduceDriver<PartitionDataWritable, AdapterWithObjectWritable, GeoWaveInputKey, ObjectWritable> reduceDriver;
  SimpleFeatureType ftype;
  final GeometryFactory factory = new GeometryFactory(new PrecisionModel(0.000001), 4326);
  short adapterId = 1234;
  final NNMapReduce.NNMapper<ClusterItem> nnMapper = new NNMapReduce.NNMapper<>();
  final NNMapReduce.NNReducer<ClusterItem, GeoWaveInputKey, ObjectWritable, Map<ByteArray, Cluster>> nnReducer =
      new DBScanMapReduce.DBScanMapHullReducer();

  @Before
  public void setUp() throws IOException {

    mapDriver = MapDriver.newMapDriver(nnMapper);
    reduceDriver = ReduceDriver.newReduceDriver(nnReducer);

    mapDriver.getConfiguration().set(
        GeoWaveConfiguratorBase.enumToConfKey(
            NNMapReduce.class,
            PartitionParameters.Partition.DISTANCE_THRESHOLDS),
        "10,10");

    reduceDriver.getConfiguration().setDouble(
        GeoWaveConfiguratorBase.enumToConfKey(
            NNMapReduce.class,
            PartitionParameters.Partition.MAX_DISTANCE),
        10);

    ftype =
        AnalyticFeature.createGeometryFeatureAdapter(
            "centroid",
            new String[] {"extra1"},
            BasicFeatureTypes.DEFAULT_NAMESPACE,
            ClusteringUtils.CLUSTERING_CRS).getFeatureType();

    reduceDriver.getConfiguration().setClass(
        GeoWaveConfiguratorBase.enumToConfKey(
            DBScanMapReduce.class,
            HullParameters.Hull.PROJECTION_CLASS),
        SimpleFeatureProjection.class,
        Projection.class);

    final FeatureDataAdapter adapter = new FeatureDataAdapter(ftype);
    JobContextAdapterStore.addDataAdapter(mapDriver.getConfiguration(), adapter);

    JobContextAdapterStore.addDataAdapter(reduceDriver.getConfiguration(), adapter);
    JobContextInternalAdapterStore.addTypeName(
        mapDriver.getConfiguration(),
        adapter.getTypeName(),
        adapterId);
    JobContextInternalAdapterStore.addTypeName(
        reduceDriver.getConfiguration(),
        adapter.getTypeName(),
        adapterId);
    serializations();
  }

  private SimpleFeature createTestFeature(final String name, final Coordinate coord) {
    return AnalyticFeature.createGeometryFeature(
        ftype,
        "b1",
        name,
        name,
        "NA",
        20.30203,
        factory.createPoint(coord),
        new String[] {"extra1"},
        new double[] {0.022},
        1,
        1,
        0);
  }

  private void serializations() {
    final String[] strings = reduceDriver.getConfiguration().getStrings("io.serializations");
    final String[] newStrings = new String[strings.length + 1];
    System.arraycopy(strings, 0, newStrings, 0, strings.length);
    newStrings[newStrings.length - 1] = SimpleFeatureImplSerialization.class.getName();
    reduceDriver.getConfiguration().setStrings("io.serializations", newStrings);

    mapDriver.getConfiguration().setStrings("io.serializations", newStrings);
  }

  @Test
  public void testReducer() throws IOException {

    final SimpleFeature feature1 = createTestFeature("f1", new Coordinate(30.0, 30.00000001));
    final SimpleFeature feature2 = createTestFeature("f2", new Coordinate(50.001, 50.001));
    final SimpleFeature feature3 =
        createTestFeature("f3", new Coordinate(30.00000001, 30.00000001));
    final SimpleFeature feature4 = createTestFeature("f4", new Coordinate(50.0011, 50.00105));
    final SimpleFeature feature5 = createTestFeature("f5", new Coordinate(50.00112, 50.00111));
    final SimpleFeature feature6 =
        createTestFeature("f6", new Coordinate(30.00000001, 30.00000002));
    final SimpleFeature feature7 = createTestFeature("f7", new Coordinate(50.00113, 50.00114));
    final SimpleFeature feature8 =
        createTestFeature("f8", new Coordinate(40.00000001, 40.000000002));

    mapDriver.addInput(new GeoWaveInputKey(adapterId, new ByteArray(feature1.getID())), feature1);
    mapDriver.addInput(new GeoWaveInputKey(adapterId, new ByteArray(feature2.getID())), feature2);
    mapDriver.addInput(new GeoWaveInputKey(adapterId, new ByteArray(feature3.getID())), feature3);
    mapDriver.addInput(new GeoWaveInputKey(adapterId, new ByteArray(feature4.getID())), feature4);
    mapDriver.addInput(new GeoWaveInputKey(adapterId, new ByteArray(feature5.getID())), feature5);
    mapDriver.addInput(new GeoWaveInputKey(adapterId, new ByteArray(feature6.getID())), feature6);
    mapDriver.addInput(new GeoWaveInputKey(adapterId, new ByteArray(feature7.getID())), feature7);
    mapDriver.addInput(new GeoWaveInputKey(adapterId, new ByteArray(feature8.getID())), feature8);

    final List<Pair<PartitionDataWritable, AdapterWithObjectWritable>> mapperResults =
        mapDriver.run();
    assertNotNull(getPartitionDataFor(mapperResults, feature1.getID(), true));
    assertNotNull(getPartitionDataFor(mapperResults, feature2.getID(), true));
    assertNotNull(getPartitionDataFor(mapperResults, feature2.getID(), true));
    assertNotNull(getPartitionDataFor(mapperResults, feature3.getID(), true));

    assertEquals(
        getPartitionDataFor(mapperResults, feature1.getID(), true).getCompositeKey(),
        getPartitionDataFor(mapperResults, feature3.getID(), true).getCompositeKey());

    assertEquals(
        getPartitionDataFor(mapperResults, feature6.getID(), true).getCompositeKey(),
        getPartitionDataFor(mapperResults, feature3.getID(), true).getCompositeKey());

    assertEquals(
        getPartitionDataFor(mapperResults, feature5.getID(), true).getCompositeKey(),
        getPartitionDataFor(mapperResults, feature7.getID(), true).getCompositeKey());

    assertEquals(
        getPartitionDataFor(mapperResults, feature5.getID(), true).getCompositeKey(),
        getPartitionDataFor(mapperResults, feature4.getID(), true).getCompositeKey());

    final List<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>> partitions =
        getReducerDataFromMapperInput(mapperResults);

    reduceDriver.addAll(partitions);

    reduceDriver.getConfiguration().setInt(
        GeoWaveConfiguratorBase.enumToConfKey(
            NNMapReduce.class,
            ClusteringParameters.Clustering.MINIMUM_SIZE),
        2);

    final List<Pair<GeoWaveInputKey, ObjectWritable>> reduceResults = reduceDriver.run();

    assertEquals(2, reduceResults.size());

    /*
     * assertEquals( feature3.getID(), find( reduceResults, feature1.getID()).toString());
     *
     * assertEquals( feature1.getID(), find( reduceResults, feature3.getID()).toString());
     *
     * assertEquals( feature4.getID(), find( reduceResults, feature2.getID()).toString());
     *
     * assertEquals( feature2.getID(), find( reduceResults, feature4.getID()).toString());
     */
  }

  private List<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>> getReducerDataFromMapperInput(
      final List<Pair<PartitionDataWritable, AdapterWithObjectWritable>> mapperResults) {
    final List<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>> reducerInputSet =
        new ArrayList<>();
    for (final Pair<PartitionDataWritable, AdapterWithObjectWritable> pair : mapperResults) {
      getListFor(pair.getFirst(), reducerInputSet).add(pair.getSecond());
    }
    return reducerInputSet;
  }

  private List<AdapterWithObjectWritable> getListFor(
      final PartitionDataWritable pd,
      final List<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>> reducerInputSet) {
    for (final Pair<PartitionDataWritable, List<AdapterWithObjectWritable>> pair : reducerInputSet) {
      if (pair.getFirst().compareTo(pd) == 0) {
        return pair.getSecond();
      }
    }
    final List<AdapterWithObjectWritable> newPairList = new ArrayList<>();
    reducerInputSet.add(new Pair(pd, newPairList));
    return newPairList;
  }

  private PartitionData getPartitionDataFor(
      final List<Pair<PartitionDataWritable, AdapterWithObjectWritable>> mapperResults,
      final String id,
      final boolean primary) {
    for (final Pair<PartitionDataWritable, AdapterWithObjectWritable> pair : mapperResults) {
      if (((FeatureWritable) pair.getSecond().getObjectWritable().get()).getFeature().getID().equals(
          id) && (pair.getFirst().getPartitionData().isPrimary() == primary)) {
        return pair.getFirst().getPartitionData();
      }
    }
    return null;
  }

  private double round(final double value) {
    return (double) Math.round(value * 1000000) / 1000000;
  }

  @Test
  public void test8With4() throws IOException {

    final Random r = new Random(3434);
    for (int i = 0; i < 8; i++) {
      final SimpleFeature feature =
          createTestFeature(
              "f" + i,
              new Coordinate(
                  round(30.0 + (r.nextGaussian() * 0.00001)),
                  round(30.0 + (r.nextGaussian() * 0.00001))));
      mapDriver.addInput(new GeoWaveInputKey(adapterId, new ByteArray(feature.getID())), feature);
    }

    final List<Pair<PartitionDataWritable, AdapterWithObjectWritable>> mapperResults =
        mapDriver.run();

    final List<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>> partitions =
        getReducerDataFromMapperInput(mapperResults);

    reduceDriver.addAll(partitions);

    reduceDriver.getConfiguration().setInt(
        GeoWaveConfiguratorBase.enumToConfKey(
            NNMapReduce.class,
            ClusteringParameters.Clustering.MINIMUM_SIZE),
        4);

    final List<Pair<GeoWaveInputKey, ObjectWritable>> reduceResults = reduceDriver.run();
    assertEquals(1, reduceResults.size());
  }

  @Test
  public void testScale() throws IOException {

    final Random r = new Random(3434);
    for (int i = 0; i < 10000; i++) {
      final SimpleFeature feature =
          createTestFeature(
              "f" + i,
              new Coordinate(
                  round(30.0 + (r.nextGaussian() * 0.0001)),
                  round(30.0 + (r.nextGaussian() * 0.0001))));
      mapDriver.addInput(new GeoWaveInputKey(adapterId, new ByteArray(feature.getID())), feature);
    }

    final List<Pair<PartitionDataWritable, AdapterWithObjectWritable>> mapperResults =
        mapDriver.run();

    final List<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>> partitions =
        getReducerDataFromMapperInput(mapperResults);

    reduceDriver.addAll(partitions);

    reduceDriver.getConfiguration().setInt(
        GeoWaveConfiguratorBase.enumToConfKey(
            NNMapReduce.class,
            ClusteringParameters.Clustering.MINIMUM_SIZE),
        10);

    final List<Pair<GeoWaveInputKey, ObjectWritable>> reduceResults = reduceDriver.run();
    assertTrue(reduceResults.size() > 0);
  }
}
