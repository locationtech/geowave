/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.analytic.mapreduce.kmeans;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import org.junit.Test;
import org.slf4j.Logger;

import com.vividsolutions.jts.geom.Coordinate;

import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.extract.CentroidExtractor;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.SampleParameters;
import mil.nga.giat.geowave.analytic.param.StoreParameters.StoreParam;
import mil.nga.giat.geowave.analytic.sample.function.SamplingRankFunction;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.memory.MemoryRequiredOptions;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

public class KSamplerMapReduceTest
{
	private static final String TEST_NAMESPACE = "test";
	MapDriver<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable> mapDriver;
	ReduceDriver<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, TestObject> reduceDriver;
	final TestObjectDataAdapter testObjectAapter = new TestObjectDataAdapter();

	private static final List<Object> capturedObjects = new ArrayList<Object>();

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
		final KSamplerMapReduce.SampleMap<TestObject> mapper = new KSamplerMapReduce.SampleMap<TestObject>();
		final KSamplerMapReduce.SampleReducer<TestObject> reducer = new KSamplerMapReduce.SampleReducer<TestObject>();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		final DataAdapter adapter = AnalyticFeature.createGeometryFeatureAdapter(
				"altoids",
				new String[] {},
				"http://geowave.test.net",
				ClusteringUtils.CLUSTERING_CRS);

		final PropertyManagement propManagement = new PropertyManagement();

		DataStorePluginOptions pluginOptions = new DataStorePluginOptions();
		GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
				"memory",
				new MemoryStoreFactoryFamily());
		pluginOptions.selectPlugin("memory");
		MemoryRequiredOptions opts = (MemoryRequiredOptions) pluginOptions.getFactoryOptions();
		opts.setGeowaveNamespace(TEST_NAMESPACE);
		PersistableStore store = new PersistableStore(
				pluginOptions);

		propManagement.store(
				StoreParam.INPUT_STORE,
				store);

		propManagement.store(
				CentroidParameters.Centroid.INDEX_ID,
				new SpatialDimensionalityTypeProvider().createPrimaryIndex(
						new SpatialOptions()).getId().getString());
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

		mapDriver.getConfiguration().setClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						KSamplerMapReduce.class,
						SampleParameters.Sample.SAMPLE_RANK_FUNCTION),
				TestSamplingMidRankFunction.class,
				SamplingRankFunction.class);

		JobContextAdapterStore.addDataAdapter(
				mapDriver.getConfiguration(),
				testObjectAapter);

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
				testObjectAapter);

		reduceDriver.getConfiguration().set(
				GeoWaveConfiguratorBase.enumToConfKey(
						KSamplerMapReduce.class,
						SampleParameters.Sample.DATA_TYPE_ID),
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
		inputKey.setAdapterId(testObjectAapter.getAdapterId());
		inputKey.setDataId(new ByteArrayId(
				"abc".getBytes()));

		final ObjectWritable ow = new ObjectWritable();
		ow.set(new TestObjectWritable(
				new TestObject(
						new Coordinate(
								25.4,
								25.6),
						"abc")));

		final GeoWaveInputKey outputKey = new GeoWaveInputKey();
		outputKey.setAdapterId(testObjectAapter.getAdapterId());

		final ByteBuffer keyBuf = ByteBuffer.allocate(64);
		keyBuf.putDouble(0.5);
		keyBuf.putInt(1);
		keyBuf.put("1".getBytes());
		keyBuf.putInt(3);
		keyBuf.put(inputKey.getDataId().getBytes());
		outputKey.setDataId(new ByteArrayId(
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
		inputKey.setAdapterId(testObjectAapter.getAdapterId());
		inputKey.setDataId(new ByteArrayId(
				"abc".getBytes()));

		final ObjectWritable ow = new ObjectWritable();
		ow.set(new TestObjectWritable(
				new TestObject(
						new Coordinate(
								25.4,
								25.6),
						"abc")));

		final GeoWaveInputKey outputKey = new GeoWaveInputKey();
		outputKey.setAdapterId(testObjectAapter.getAdapterId());

		final ByteBuffer keyBuf = ByteBuffer.allocate(64);
		keyBuf.putDouble(0.0);
		keyBuf.putInt(3);
		keyBuf.put(inputKey.getDataId().getBytes());
		outputKey.setDataId(new ByteArrayId(
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
		inputKey1.setAdapterId(testObjectAapter.getAdapterId());

		ByteBuffer keyBuf = ByteBuffer.allocate(64);
		keyBuf.putDouble(0.5);
		keyBuf.putInt(3);
		keyBuf.put("111".getBytes());
		inputKey1.setDataId(new ByteArrayId(
				keyBuf.array()));

		keyBuf = ByteBuffer.allocate(64);
		final GeoWaveInputKey inputKey2 = new GeoWaveInputKey();
		inputKey2.setAdapterId(testObjectAapter.getAdapterId());
		keyBuf.putDouble(0.6);
		keyBuf.putInt(3);
		keyBuf.put("111".getBytes());
		inputKey2.setDataId(new ByteArrayId(
				keyBuf.array()));

		keyBuf = ByteBuffer.allocate(64);
		final GeoWaveInputKey inputKey3 = new GeoWaveInputKey();
		inputKey3.setAdapterId(testObjectAapter.getAdapterId());
		keyBuf.putDouble(0.7);
		keyBuf.putInt(3);
		keyBuf.put("111".getBytes());
		inputKey3.setDataId(new ByteArrayId(
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
		assertTrue(Arrays.equals(
				results.get(
						0).getFirst().getAdapterId().getBytes(),
				"altoids".getBytes()));
		assertTrue(Arrays.equals(
				results.get(
						1).getFirst().getAdapterId().getBytes(),
				"altoids".getBytes()));
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
