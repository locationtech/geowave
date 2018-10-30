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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.FeatureWritable;
import org.locationtech.geowave.analytic.AnalyticFeature;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.SimpleFeatureItemWrapperFactory;
import org.locationtech.geowave.analytic.clustering.CentroidManagerGeoWave;
import org.locationtech.geowave.analytic.clustering.ClusteringUtils;
import org.locationtech.geowave.analytic.clustering.DistortionGroupManagement.DistortionEntry;
import org.locationtech.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import org.locationtech.geowave.analytic.distance.DistanceFn;
import org.locationtech.geowave.analytic.distance.FeatureCentroidDistanceFn;
import org.locationtech.geowave.analytic.extract.SimpleFeatureCentroidExtractor;
import org.locationtech.geowave.analytic.mapreduce.CountofDoubleWritable;
import org.locationtech.geowave.analytic.param.CentroidParameters;
import org.locationtech.geowave.analytic.param.CommonParameters;
import org.locationtech.geowave.analytic.param.GlobalParameters;
import org.locationtech.geowave.analytic.param.StoreParameters.StoreParam;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import org.locationtech.geowave.mapreduce.GeoWaveConfiguratorBase;
import org.locationtech.geowave.mapreduce.JobContextAdapterStore;
import org.locationtech.geowave.mapreduce.JobContextInternalAdapterStore;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class KMeansDistortionMapReduceTest
{
	MapDriver<GeoWaveInputKey, ObjectWritable, Text, CountofDoubleWritable> mapDriver;
	ReduceDriver<Text, CountofDoubleWritable, GeoWaveOutputKey, DistortionEntry> reduceDriver;
	@Rule
	public TestName name = new TestName();

	final String batchId = "b1";

	final SimpleFeatureType ftype = AnalyticFeature.createGeometryFeatureAdapter(
			"centroid",
			new String[] {
				"extra1"
			},
			"http://geowave.test.net",
			ClusteringUtils.CLUSTERING_CRS).getFeatureType();
	final FeatureDataAdapter testObjectAdapter = new FeatureDataAdapter(
			ftype);
	short adapterId = 1234;

	private static final List<Object> capturedObjects = new ArrayList<>();

	final Index index = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
	final GeometryFactory factory = new GeometryFactory();
	final String grp1 = "g1";

	@Before
	public void setUp()
			throws IOException {
		final KMeansDistortionMapReduce.KMeansDistortionMapper mapper = new KMeansDistortionMapReduce.KMeansDistortionMapper();
		final KMeansDistortionMapReduce.KMeansDistortionReduce reducer = new KMeansDistortionMapReduce.KMeansDistortionReduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);

		mapDriver.getConfiguration().setClass(
				GeoWaveConfiguratorBase.enumToConfKey(
						KMeansDistortionMapReduce.class,
						CommonParameters.Common.DISTANCE_FUNCTION_CLASS),
				FeatureCentroidDistanceFn.class,
				DistanceFn.class);
		testObjectAdapter.init(index);
		JobContextAdapterStore.addDataAdapter(
				mapDriver.getConfiguration(),
				testObjectAdapter);

		JobContextAdapterStore.addDataAdapter(
				reduceDriver.getConfiguration(),
				testObjectAdapter);

		JobContextInternalAdapterStore.addTypeName(
				mapDriver.getConfiguration(),
				testObjectAdapter.getTypeName(),
				adapterId);
		JobContextInternalAdapterStore.addTypeName(
				reduceDriver.getConfiguration(),
				testObjectAdapter.getTypeName(),
				adapterId);
		final PropertyManagement propManagement = new PropertyManagement();
		propManagement.store(
				CentroidParameters.Centroid.INDEX_NAME,
				new SpatialDimensionalityTypeProvider().createIndex(
						new SpatialOptions()).getName());
		propManagement.store(
				CentroidParameters.Centroid.DATA_TYPE_ID,
				ftype.getTypeName());

		propManagement.store(
				CentroidParameters.Centroid.DATA_NAMESPACE_URI,
				ftype.getName().getNamespaceURI());
		propManagement.store(
				GlobalParameters.Global.BATCH_ID,
				batchId);
		propManagement.store(
				CentroidParameters.Centroid.EXTRACTOR_CLASS,
				SimpleFeatureCentroidExtractor.class);
		propManagement.store(
				CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
				SimpleFeatureItemWrapperFactory.class);

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

		NestedGroupCentroidAssignment.setParameters(
				mapDriver.getConfiguration(),
				KMeansDistortionMapReduce.class,
				propManagement);

		serializations();

		capturedObjects.clear();

		final SimpleFeature feature = AnalyticFeature.createGeometryFeature(
				ftype,
				batchId,
				"123",
				"fred",
				grp1,
				20.30203,
				factory.createPoint(new Coordinate(
						02.33,
						0.23)),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				1,
				1,
				0);

		propManagement.store(
				CentroidParameters.Centroid.ZOOM_LEVEL,
				1);
		ingest(
				pluginOptions.createDataStore(),
				testObjectAdapter,
				index,
				feature);

		CentroidManagerGeoWave.setParameters(
				reduceDriver.getConfiguration(),
				KMeansDistortionMapReduce.class,
				propManagement);
	}

	private void ingest(
			final DataStore dataStore,
			final FeatureDataAdapter adapter,
			final Index index,
			final SimpleFeature feature )
			throws IOException {
		dataStore.addType(
				adapter,
				index);
		try (Writer writer = dataStore.createWriter(adapter.getTypeName())) {
			writer.write(feature);
			writer.close();
		}
	}

	private void serializations() {
		final String[] strings = reduceDriver.getConfiguration().getStrings(
				"io.serializations");
		final String[] newStrings = new String[strings.length + 1];
		System.arraycopy(
				strings,
				0,
				newStrings,
				0,
				strings.length);
		newStrings[newStrings.length - 1] = SimpleFeatureImplSerialization.class.getName();
		reduceDriver.getConfiguration().setStrings(
				"io.serializations",
				newStrings);

		mapDriver.getConfiguration().setStrings(
				"io.serializations",
				newStrings);
	}

	@Test
	public void testMapper()
			throws IOException {

		final GeoWaveInputKey inputKey = new GeoWaveInputKey();
		inputKey.setInternalAdapterId(adapterId);
		inputKey.setDataId(new ByteArray(
				"abc".getBytes()));

		final ObjectWritable ow = new ObjectWritable();
		ow.set(new FeatureWritable(
				ftype,
				AnalyticFeature.createGeometryFeature(
						ftype,
						batchId,
						"123",
						"fred",
						grp1,
						20.30203,
						factory.createPoint(new Coordinate(
								02.33,
								0.23)),
						new String[] {
							"extra1"
						},
						new double[] {
							0.022
						},
						1,
						1,
						0)));

		mapDriver.withInput(
				inputKey,
				ow);

		final List<Pair<Text, CountofDoubleWritable>> results = mapDriver.run();
		// output key has the dataID adjusted to contain the rank
		assertEquals(
				results.get(
						0).getFirst().toString(),
				grp1);
		// output value is the same as input value
		assertEquals(
				results.get(
						0).getSecond().getValue(),
				0.0,
				0.0001);

	}

	@Test
	public void testReducer()
			throws IOException {

		reduceDriver.addInput(
				new Text(
						"g1"),
				Arrays.asList(
						new CountofDoubleWritable(
								0.34,
								1),
						new CountofDoubleWritable(
								0.75,
								1)));
		reduceDriver.addInput(
				new Text(
						"g2"),
				Arrays.asList(
						new CountofDoubleWritable(
								0.34,
								1),
						new CountofDoubleWritable(
								0.25,
								1)));

		final List<Pair<GeoWaveOutputKey, DistortionEntry>> results = reduceDriver.run();
		assertEquals(
				1,
				results.size());

		assertTrue(results.get(
				0).getSecond().getGroupId().equals(
				"g1"));
		assertTrue(results.get(
				0).getSecond().getClusterCount().equals(
				1));
		// TODO: floating point error?
		assertTrue(results.get(
				0).getSecond().getDistortionValue().equals(
				3.6697247706422016));
	}
}
