/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.nn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputByteBuffer;
import org.apache.hadoop.io.Text;
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
import org.locationtech.geowave.analytic.clustering.ClusteringUtils;
import org.locationtech.geowave.analytic.distance.DistanceFn;
import org.locationtech.geowave.analytic.distance.FeatureCentroidOrthodromicDistanceFn;
import org.locationtech.geowave.analytic.mapreduce.kmeans.SimpleFeatureImplSerialization;
import org.locationtech.geowave.analytic.mapreduce.nn.NNMapReduce.PartitionDataWritable;
import org.locationtech.geowave.analytic.param.CommonParameters;
import org.locationtech.geowave.analytic.param.PartitionParameters;
import org.locationtech.geowave.analytic.partitioner.Partitioner.PartitionData;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.mapreduce.GeoWaveConfiguratorBase;
import org.locationtech.geowave.mapreduce.JobContextAdapterStore;
import org.locationtech.geowave.mapreduce.JobContextInternalAdapterStore;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class NNMapReduceTest {

  MapDriver<GeoWaveInputKey, Object, PartitionDataWritable, AdapterWithObjectWritable> mapDriver;
  ReduceDriver<PartitionDataWritable, AdapterWithObjectWritable, Text, Text> reduceDriver;
  SimpleFeatureType ftype;
  short internalAdapterId;
  final GeometryFactory factory = new GeometryFactory();

  @Before
  public void setUp() throws IOException {
    GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
        "memory",
        new MemoryStoreFactoryFamily());
    final NNMapReduce.NNMapper<SimpleFeature> nnMapper = new NNMapReduce.NNMapper<>();
    final NNMapReduce.NNReducer<SimpleFeature, Text, Text, Boolean> nnReducer =
        new NNMapReduce.NNSimpleFeatureIDOutputReducer();

    mapDriver = MapDriver.newMapDriver(nnMapper);
    reduceDriver = ReduceDriver.newReduceDriver(nnReducer);

    mapDriver.getConfiguration().set(
        GeoWaveConfiguratorBase.enumToConfKey(
            NNMapReduce.class,
            PartitionParameters.Partition.DISTANCE_THRESHOLDS),
        "0.0002,0.0002");

    reduceDriver.getConfiguration().setClass(
        GeoWaveConfiguratorBase.enumToConfKey(
            NNMapReduce.class,
            CommonParameters.Common.DISTANCE_FUNCTION_CLASS),
        FeatureCentroidOrthodromicDistanceFn.class,
        DistanceFn.class);
    reduceDriver.getConfiguration().setDouble(
        GeoWaveConfiguratorBase.enumToConfKey(
            NNMapReduce.class,
            PartitionParameters.Partition.MAX_DISTANCE),
        0.001);

    ftype =
        AnalyticFeature.createGeometryFeatureAdapter(
            "centroid",
            new String[] {"extra1"},
            BasicFeatureTypes.DEFAULT_NAMESPACE,
            ClusteringUtils.CLUSTERING_CRS).getFeatureType();

    final Index index = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
    final FeatureDataAdapter adapter = new FeatureDataAdapter(ftype);
    adapter.init(index);

    JobContextAdapterStore.addDataAdapter(mapDriver.getConfiguration(), adapter);
    internalAdapterId = InternalAdapterStoreImpl.getLazyInitialAdapterId(adapter.getTypeName());
    JobContextAdapterStore.addDataAdapter(reduceDriver.getConfiguration(), adapter);
    JobContextInternalAdapterStore.addTypeName(
        mapDriver.getConfiguration(),
        adapter.getTypeName(),
        internalAdapterId);
    JobContextInternalAdapterStore.addTypeName(
        reduceDriver.getConfiguration(),
        adapter.getTypeName(),
        internalAdapterId);

    serializations();
  }

  private SimpleFeature createTestFeature(final Coordinate coord) {
    return AnalyticFeature.createGeometryFeature(
        ftype,
        "b1",
        UUID.randomUUID().toString(),
        "fred",
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
  public void testMapper() throws IOException {

    final SimpleFeature feature1 = createTestFeature(new Coordinate(30.0, 30.00000001));
    final SimpleFeature feature2 = createTestFeature(new Coordinate(179.9999999999, 30.0000001));
    final SimpleFeature feature3 = createTestFeature(new Coordinate(30.00000001, 30.00000001));
    final SimpleFeature feature4 = createTestFeature(new Coordinate(-179.9999999999, 30.0000001));

    final GeoWaveInputKey inputKey1 = new GeoWaveInputKey();
    inputKey1.setInternalAdapterId(internalAdapterId);
    inputKey1.setDataId(new ByteArray(feature1.getID()));

    final GeoWaveInputKey inputKey2 = new GeoWaveInputKey();
    inputKey2.setInternalAdapterId(internalAdapterId);
    inputKey2.setDataId(new ByteArray(feature2.getID()));

    final GeoWaveInputKey inputKey3 = new GeoWaveInputKey();
    inputKey3.setInternalAdapterId(internalAdapterId);
    inputKey3.setDataId(new ByteArray(feature4.getID()));

    final GeoWaveInputKey inputKey4 = new GeoWaveInputKey();
    inputKey4.setInternalAdapterId(internalAdapterId);
    inputKey4.setDataId(new ByteArray(feature4.getID()));

    mapDriver.addInput(inputKey1, feature1);
    mapDriver.addInput(inputKey2, feature2);
    mapDriver.addInput(inputKey3, feature3);
    mapDriver.addInput(inputKey4, feature4);
    final List<Pair<PartitionDataWritable, AdapterWithObjectWritable>> mapperResults =
        mapDriver.run();
    assertEquals(
        10, // includes overlap
        mapperResults.size());
    assertFalse(getPartitionDataFor(mapperResults, feature1.getID(), true).isEmpty());
    assertFalse(getPartitionDataFor(mapperResults, feature2.getID(), true).isEmpty());
    assertFalse(getPartitionDataFor(mapperResults, feature2.getID(), false).isEmpty());
    assertFalse(getPartitionDataFor(mapperResults, feature3.getID(), true).isEmpty());

    assertTrue(
        intersects(
            getPartitionDataFor(mapperResults, feature1.getID(), true),
            getPartitionDataFor(mapperResults, feature3.getID(), true)));

    assertTrue(
        intersects(
            getPartitionDataFor(mapperResults, feature2.getID(), false),
            getPartitionDataFor(mapperResults, feature4.getID(), false)));

    final List<Pair<PartitionDataWritable, List<AdapterWithObjectWritable>>> partitions =
        getReducerDataFromMapperInput(mapperResults);
    assertEquals(3, partitions.size());

    reduceDriver.addAll(partitions);

    final List<Pair<Text, Text>> reduceResults = reduceDriver.run();

    assertEquals(4, reduceResults.size());

    assertEquals(feature3.getID(), find(reduceResults, feature1.getID()).toString());

    assertEquals(feature1.getID(), find(reduceResults, feature3.getID()).toString());

    assertEquals(feature4.getID(), find(reduceResults, feature2.getID()).toString());

    assertEquals(feature2.getID(), find(reduceResults, feature4.getID()).toString());
  }

  @Test
  public void testWritable() throws IOException {

    final PartitionDataWritable writable1 = new PartitionDataWritable();
    final PartitionDataWritable writable2 = new PartitionDataWritable();

    writable1.setPartitionData(
        new PartitionData(new ByteArray(new byte[] {}), new ByteArray("abc"), true));
    writable2.setPartitionData(
        new PartitionData(new ByteArray(new byte[] {}), new ByteArray("abc"), false));

    assertTrue(writable1.compareTo(writable2) == 0);
    writable2.setPartitionData(
        new PartitionData(new ByteArray(new byte[] {}), new ByteArray("abd"), false));
    assertTrue(writable1.compareTo(writable2) < 0);
    writable2.setPartitionData(
        new PartitionData(new ByteArray(new byte[] {}), new ByteArray("abd"), true));
    assertTrue(writable1.compareTo(writable2) < 0);

    final DataOutputByteBuffer output = new DataOutputByteBuffer();
    writable1.write(output);
    output.flush();
    final DataInputByteBuffer input = new DataInputByteBuffer();
    input.reset(output.getData());

    writable2.readFields(input);
    assertTrue(writable1.compareTo(writable2) == 0);
  }

  private Text find(final List<Pair<Text, Text>> outputSet, final String key) {
    for (final Pair<Text, Text> item : outputSet) {
      if (key.equals(item.getFirst().toString())) {
        return item.getSecond();
      }
    }
    return null;
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

  private boolean intersects(final List<PartitionData> setOne, final List<PartitionData> setTwo) {
    for (final PartitionData pdOne : setOne) {
      for (final PartitionData pdTwo : setTwo) {
        if (pdOne.getCompositeKey().equals(pdTwo.getCompositeKey())) {
          return true;
        }
      }
    }
    return false;
  }

  private List<PartitionData> getPartitionDataFor(
      final List<Pair<PartitionDataWritable, AdapterWithObjectWritable>> mapperResults,
      final String id,
      final boolean primary) {
    final ArrayList<PartitionData> results = new ArrayList<>();
    for (final Pair<PartitionDataWritable, AdapterWithObjectWritable> pair : mapperResults) {
      if (((FeatureWritable) pair.getSecond().getObjectWritable().get()).getFeature().getID().equals(
          id) && (pair.getFirst().partitionData.isPrimary() == primary)) {
        results.add(pair.getFirst().partitionData);
      }
    }
    return results;
  }
}
