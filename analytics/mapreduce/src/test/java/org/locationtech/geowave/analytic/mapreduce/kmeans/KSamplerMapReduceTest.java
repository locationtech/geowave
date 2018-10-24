/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.analytic.mapreduce.kmeans;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.locationtech.geowave.analytic.AnalyticFeature;
import org.locationtech.geowave.analytic.AnalyticItemWrapperFactory;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.clustering.CentroidManagerGeoWave;
import org.locationtech.geowave.analytic.clustering.ClusteringUtils;
import org.locationtech.geowave.analytic.extract.CentroidExtractor;
import org.locationtech.geowave.analytic.param.CentroidParameters;
import org.locationtech.geowave.analytic.param.GlobalParameters;
import org.locationtech.geowave.analytic.param.SampleParameters;
import org.locationtech.geowave.analytic.param.StoreParameters.StoreParam;
import org.locationtech.geowave.analytic.sample.function.SamplingRankFunction;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.mapreduce.GeoWaveConfiguratorBase;
import org.locationtech.geowave.mapreduce.JobContextAdapterStore;
import org.locationtech.geowave.mapreduce.JobContextInternalAdapterStore;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;
import org.slf4j.Logger;

import com.vividsolutions.jts.geom.Coordinate;

public class KSamplerMapReduceTest
{
	MapDriver<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable> mapDriver;
	ReduceDriver<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, TestObject> reduceDriver;
	short internalAdapterId;
	short other;
	final TestObjectDataAdapter testObjectAdapter = new TestObjectDataAdapter();
	@Rule
	public TestName name = new TestName();

	private static final List<Object> capturedObjects = new ArrayList<>();

	public KSamplerMapReduceTest() {}

	public static class TestSamplingMidRankFunction implements
			SamplingRankFunction
	{

		@Override
		public double rank(
				final int sampleSize,
				final Object value ) {
			capturedObjects.add(value);
			return 0.5;
		}

		@Override
		public void initialize(
				final JobContext context,
				final Class scope,
				final Logger logger )
				throws IOException {}
	}

	public static class TestSamplingNoRankFunction implements
			SamplingRankFunction
	{
		@Override
		public void initialize(
				final JobContext context,
				final Class scope,
				final Logger logger )
				throws IOException {}

		@Override
		public double rank(
				final int sampleSize,
				final Object value ) {
			capturedObjects.add(value);
			return 0.0;
		}
	}

	@Before
	public void setUp()
			throws IOException {
		final KSamplerMapReduce.SampleMap<TestObject> mapper = new KSamplerMapReduce.SampleMap<>();
		final KSamplerMapReduce.SampleReducer<TestObject> reducer = new KSamplerMapReduce.SampleReducer<>();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		final DataTypeAdapter<?> adapter = AnalyticFeature.createGeometryFeatureAdapter(
				"altoids",
				new String[] {},
				"http://geowave.test.net",
				ClusteringUtils.CLUSTERING_CRS);

		final PropertyManagement propManagement = new PropertyManagement();

		final DataStorePluginOptions pluginOptions = new DataStorePluginOptions();
		GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
				"memory",
				new MemoryStoreFactoryFamily());
		pluginOptions.selectPlugin("memory");
		final MemoryRequiredOptions opts = (MemoryRequiredOptions) pluginOptions.getFactoryOptions();
		final String namespace = "test_" + getClass().getName() + "_" + name.getMethodName();
		opts.setGeowaveNamespace(namespace);
		final PersistableStore store = new PersistableStore(
				pluginOptions);

		propManagement.store(
				StoreParam.INPUT_STORE,
				store);

		propManagement.store(
				CentroidParameters.Centroid.INDEX_NAME,
				new SpatialDimensionalityTypeProvider().createIndex(
						new SpatialOptions()).getName());
		propManagement.store(
				CentroidParameters.Centroid.DATA_TYPE_ID,
				"altoids");
		propManagement.store(
				CentroidParameters.Centroid.DATA_NAMESPACE_URI,
				"http://geowave.test.net");
		propManagement.store(
				GlobalParameters.Global.BATCH_ID,
				"b1");
		propManagement.store(
				CentroidParameters.Centroid.EXTRACTOR_CLASS,
				TestObjectExtractor.class);
		propManagement.store(
				CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
				TestObjectItemWrapperFactory.class);

		CentroidManagerGeoWave.setParameters(
				reduceDriver.getConfiguration(),
				KSamplerMapReduce.class,
				propManagement);
		CentroidManagerGeoWave.setParameters(
				mapDriver.getConfiguration(),
				KSamplerMapReduce.class,
				propManagement);
		// TODO it seems the centroid adapter is required to have been written,
		// should this initialization be handled by the runner class rather than
		// externally such as in the test?
		final DataStore dataStore = store.getDataStoreOptions().createDataStore();
		dataStore.addType(
				adapter,
				new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions()));

		mapDriver.getConfiguration().setClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						KSamplerMapReduce.class,
						SampleParameters.Sample.SAMPLE_RANK_FUNCTION),
				TestSamplingMidRankFunction.class,
				SamplingRankFunction.class);
		internalAdapterId = InternalAdapterStoreImpl.getInitialAdapterId(testObjectAdapter.getTypeName());
		other = InternalAdapterStoreImpl.getInitialAdapterId(adapter.getTypeName());
		JobContextAdapterStore.addDataAdapter(
				mapDriver.getConfiguration(),
				testObjectAdapter);
		JobContextAdapterStore.addDataAdapter(
				mapDriver.getConfiguration(),
				adapter);
		JobContextInternalAdapterStore.addTypeName(
				mapDriver.getConfiguration(),
				testObjectAdapter.getTypeName(),
				internalAdapterId);
		JobContextInternalAdapterStore.addTypeName(
				mapDriver.getConfiguration(),
				adapter.getTypeName(),
				other);

		mapDriver.getConfiguration().setInt(
				GeoWaveConfiguratorBase.enumToConfKey(
						KSamplerMapReduce.class,
						SampleParameters.Sample.SAMPLE_SIZE),
				2);

		reduceDriver.getConfiguration().setInt(
				GeoWaveConfiguratorBase.enumToConfKey(
						KSamplerMapReduce.class,
						SampleParameters.Sample.SAMPLE_SIZE),
				2);

		JobContextAdapterStore.addDataAdapter(
				reduceDriver.getConfiguration(),
				adapter);
		JobContextAdapterStore.addDataAdapter(
				reduceDriver.getConfiguration(),
				testObjectAdapter);
		JobContextInternalAdapterStore.addTypeName(
				reduceDriver.getConfiguration(),
				adapter.getTypeName(),
				other);
		JobContextInternalAdapterStore.addTypeName(
				reduceDriver.getConfiguration(),
				testObjectAdapter.getTypeName(),
				internalAdapterId);

		reduceDriver.getConfiguration().set(
				GeoWaveConfiguratorBase.enumToConfKey(
						KSamplerMapReduce.class,
						SampleParameters.Sample.DATA_TYPE_NAME),
				"altoids");

		reduceDriver.getConfiguration().setClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						KSamplerMapReduce.class,
						CentroidParameters.Centroid.EXTRACTOR_CLASS),
				TestObjectExtractor.class,
				CentroidExtractor.class);

		mapDriver.getConfiguration().setClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						KSamplerMapReduce.class,
						CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS),
				TestObjectItemWrapperFactory.class,
				AnalyticItemWrapperFactory.class);

		reduceDriver.getConfiguration().setClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						KSamplerMapReduce.class,
						CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS),
				TestObjectItemWrapperFactory.class,
				AnalyticItemWrapperFactory.class);

		serializations();
	}

	private void serializations() {
		final String[] strings = reduceDriver.getConfiguration().getStrings(
				"io.serializations");
		final String[] newStrings = new String[strings.length + 2];
		System.arraycopy(
				strings,
				0,
				newStrings,
				0,
				strings.length);
		newStrings[newStrings.length - 1] = SimpleFeatureImplSerialization.class.getName();
		newStrings[newStrings.length - 2] = TestObjectSerialization.class.getName();
		reduceDriver.getConfiguration().setStrings(
				"io.serializations",
				newStrings);
	}

	@Test
	public void testMapperWithMidRankedKey()
			throws IOException {

		capturedObjects.clear();
		mapDriver.getConfiguration().setClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						KSamplerMapReduce.class,
						SampleParameters.Sample.SAMPLE_RANK_FUNCTION),
				TestSamplingMidRankFunction.class,
				SamplingRankFunction.class);

		final GeoWaveInputKey inputKey = new GeoWaveInputKey();
		inputKey.setInternalAdapterId(internalAdapterId);
		inputKey.setDataId(new ByteArray(
				"abc".getBytes()));

		final ObjectWritable ow = new ObjectWritable();
		ow.set(new TestObjectWritable(
				new TestObject(
						new Coordinate(
								25.4,
								25.6),
						"abc")));

		final GeoWaveInputKey outputKey = new GeoWaveInputKey();
		outputKey.setInternalAdapterId(internalAdapterId);

		final ByteBuffer keyBuf = ByteBuffer.allocate(64);
		keyBuf.putDouble(0.5);
		keyBuf.putInt(1);
		keyBuf.put("1".getBytes());
		keyBuf.putInt(3);
		keyBuf.put(inputKey.getDataId().getBytes());
		outputKey.setDataId(new ByteArray(
				keyBuf.array()));

		mapDriver.withInput(
				inputKey,
				ow);

		final List<Pair<GeoWaveInputKey, ObjectWritable>> results = mapDriver.run();
		// output key has the dataID adjusted to contain the rank
		assertEquals(
				results.get(
						0).getFirst(),
				outputKey);
		// output value is the same as input value
		assertEquals(
				results.get(
						0).getSecond().get(),
				ow.get());

		// results from sample rank function to make sure it was provided the
		// correct object
		assertEquals(
				1,
				capturedObjects.size());
		assertEquals(
				"abc",
				((TestObject) capturedObjects.get(0)).id);
	}

	@Test
	public void testMapperWithZeroRank()
			throws IOException {
		capturedObjects.clear();
		mapDriver.getConfiguration().setClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						KSamplerMapReduce.class,
						SampleParameters.Sample.SAMPLE_RANK_FUNCTION),
				TestSamplingNoRankFunction.class,
				SamplingRankFunction.class);

		final GeoWaveInputKey inputKey = new GeoWaveInputKey();
		inputKey.setInternalAdapterId(internalAdapterId);
		inputKey.setDataId(new ByteArray(
				"abc".getBytes()));

		final ObjectWritable ow = new ObjectWritable();
		ow.set(new TestObjectWritable(
				new TestObject(
						new Coordinate(
								25.4,
								25.6),
						"abc")));

		final GeoWaveInputKey outputKey = new GeoWaveInputKey();
		outputKey.setInternalAdapterId(internalAdapterId);

		final ByteBuffer keyBuf = ByteBuffer.allocate(64);
		keyBuf.putDouble(0.0);
		keyBuf.putInt(3);
		keyBuf.put(inputKey.getDataId().getBytes());
		outputKey.setDataId(new ByteArray(
				keyBuf.array()));

		mapDriver.withInput(
				inputKey,
				ow);

		final List<Pair<GeoWaveInputKey, ObjectWritable>> results = mapDriver.run();

		assertEquals(
				0,
				results.size());

		// results from sample rank function to make sure it was provided the
		// correct object
		assertEquals(
				1,
				capturedObjects.size());
		assertEquals(
				"abc",
				((TestObject) capturedObjects.get(0)).id);
	}

	@Test
	public void testReducer()
			throws IOException {

		final ObjectWritable ow1 = new ObjectWritable();
		ow1.set(new TestObjectWritable(
				new TestObject(
						new Coordinate(
								25.4,
								25.6),
						"abc")));

		final ObjectWritable ow2 = new ObjectWritable();
		ow2.set(new TestObjectWritable(
				new TestObject(
						new Coordinate(
								25.4,
								25.6),
						"def")));

		final ObjectWritable ow3 = new ObjectWritable();
		ow3.set(new TestObjectWritable(
				new TestObject(
						new Coordinate(
								25.4,
								25.6),
						"ghi")));

		final GeoWaveInputKey inputKey1 = new GeoWaveInputKey();
		inputKey1.setInternalAdapterId(internalAdapterId);

		ByteBuffer keyBuf = ByteBuffer.allocate(64);
		keyBuf.putDouble(0.5);
		keyBuf.putInt(3);
		keyBuf.put("111".getBytes());
		inputKey1.setDataId(new ByteArray(
				keyBuf.array()));

		keyBuf = ByteBuffer.allocate(64);
		final GeoWaveInputKey inputKey2 = new GeoWaveInputKey();
		inputKey2.setInternalAdapterId(internalAdapterId);
		keyBuf.putDouble(0.6);
		keyBuf.putInt(3);
		keyBuf.put("111".getBytes());
		inputKey2.setDataId(new ByteArray(
				keyBuf.array()));

		keyBuf = ByteBuffer.allocate(64);
		final GeoWaveInputKey inputKey3 = new GeoWaveInputKey();
		inputKey3.setInternalAdapterId(internalAdapterId);
		keyBuf.putDouble(0.7);
		keyBuf.putInt(3);
		keyBuf.put("111".getBytes());
		inputKey3.setDataId(new ByteArray(
				keyBuf.array()));

		reduceDriver.addInput(
				inputKey1,
				Arrays.asList(ow1));

		reduceDriver.addInput(
				inputKey2,
				Arrays.asList(ow2));

		reduceDriver.addInput(
				inputKey3,
				Arrays.asList(ow3));

		final List<Pair<GeoWaveOutputKey, TestObject>> results = reduceDriver.run();
		assertEquals(
				2,
				results.size());
		assertEquals(
				results.get(
						0).getFirst().getTypeName(),
				"altoids");
		assertEquals(
				results.get(
						1).getFirst().getTypeName(),
				"altoids");
		assertEquals(
				"abc",
				results.get(
						0).getSecond().getName());
		assertEquals(
				"def",
				results.get(
						1).getSecond().getName());

	}
}
