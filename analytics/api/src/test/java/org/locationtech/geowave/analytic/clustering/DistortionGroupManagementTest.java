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
package org.locationtech.geowave.analytic.clustering;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.analytic.AnalyticFeature;
import org.locationtech.geowave.analytic.AnalyticItemWrapper;
import org.locationtech.geowave.analytic.SimpleFeatureItemWrapperFactory;
import org.locationtech.geowave.analytic.clustering.DistortionGroupManagement.DistortionDataAdapter;
import org.locationtech.geowave.analytic.clustering.DistortionGroupManagement.DistortionEntry;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class DistortionGroupManagementTest
{
	@Rule
	public TestName name = new TestName();
	final GeometryFactory factory = new GeometryFactory();
	final SimpleFeatureType ftype;
	final Index index = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());

	final FeatureDataAdapter adapter;
	final DataStorePluginOptions storePluginOptions;

	private <T> void ingest(
			final DataTypeAdapter<T> adapter,
			final Index index,
			final T entry )
			throws IOException {
		final DataStore store = storePluginOptions.createDataStore();
		store.addType(
				adapter,
				index);
		try (Writer writer = store.createWriter(adapter.getTypeName())) {
			writer.write(entry);
		}
	}

	public DistortionGroupManagementTest()
			throws MismatchedIndexToAdapterMapping,
			IOException {
		ftype = AnalyticFeature.createGeometryFeatureAdapter(
				"centroid",
				new String[] {
					"extra1"
				},
				BasicFeatureTypes.DEFAULT_NAMESPACE,
				ClusteringUtils.CLUSTERING_CRS).getFeatureType();
		adapter = new FeatureDataAdapter(
				ftype);
		adapter.init(index);
		final String namespace = "test_" + getClass().getName() + "_" + name.getMethodName();

		final StoreFactoryOptions opts = new MemoryStoreFactoryFamily().getDataStoreFactory().createOptionsInstance();
		opts.setGeowaveNamespace(namespace);
		storePluginOptions = new DataStorePluginOptions(
				opts);
		final DataStore store = storePluginOptions.createDataStore();
		store.addType(
				adapter,
				index);
	}

	private void addDistortion(
			final String grp,
			final String batchId,
			final int count,
			final Double distortion )
			throws IOException {
		ingest(
				new DistortionDataAdapter(),
				DistortionGroupManagement.DISTORTIONS_INDEX,
				new DistortionEntry(
						grp,
						batchId,
						count,
						distortion));

	}

	@Before
	public void setup()
			throws IOException {
		// big jump for grp1 between batch 2 and 3
		// big jump for grp2 between batch 1 and 2
		// thus, the jump occurs for different groups between different batches!

		// b1
		addDistortion(
				"grp1",
				"b1",
				1,
				0.1);
		addDistortion(
				"grp2",
				"b1",
				1,
				0.1);
		// b2
		addDistortion(
				"grp1",
				"b1",
				2,
				0.2);
		addDistortion(
				"grp2",
				"b1",
				2,
				0.3);
		// b3
		addDistortion(
				"grp1",
				"b1",
				3,
				0.4);
		addDistortion(
				"grp2",
				"b1",
				3,
				0.4);
		// another batch to catch wrong batch error case
		addDistortion(
				"grp1",
				"b2",
				3,
				0.05);

		ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_1",
						"123",
						"fred",
						"grp1",
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
						0));

		ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_1",
						"124",
						"barney",
						"grp1",
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
						0));

		ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_1",
						"125",
						"wilma",
						"grp2",
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
						0));

		ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_1",
						"126",
						"betty",
						"grp2",
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
						0));

		ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_2",
						"130",
						"dusty",
						"grp1",
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
						0));

		ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_2",
						"131",
						"dino",
						"grp1",
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
						0));

		ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_2",
						"127",
						"bamm-bamm",
						"grp2",
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
						0));

		ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_2",
						"128",
						"chip",
						"grp2",
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
						0));

		ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_3",
						"140",
						"pearl",
						"grp1",
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
						0));

		ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_3",
						"141",
						"roxy",
						"grp1",
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
						0));

		ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_3",
						"142",
						"giggles",
						"grp2",
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
						0));

		ingest(
				adapter,
				index,
				AnalyticFeature.createGeometryFeature(
						ftype,
						"b1_3",
						"143",
						"gazoo",
						"grp2",
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
						0));

	}

	@Test
	public void test()
			throws IOException {
		final DistortionGroupManagement distortionGroupManagement = new DistortionGroupManagement(
				storePluginOptions);
		distortionGroupManagement.retainBestGroups(
				new SimpleFeatureItemWrapperFactory(),
				adapter.getTypeName(),
				index.getName(),
				"b1",
				1);
		final CentroidManagerGeoWave<SimpleFeature> centroidManager = new CentroidManagerGeoWave<>(
				storePluginOptions.createDataStore(),
				storePluginOptions.createIndexStore(),
				storePluginOptions.createAdapterStore(),
				new SimpleFeatureItemWrapperFactory(),
				adapter.getTypeName(),
				storePluginOptions.createInternalAdapterStore().getAdapterId(
						adapter.getTypeName()),
				index.getName(),
				"b1",
				1);
		final List<String> groups = centroidManager.getAllCentroidGroups();
		assertEquals(
				2,
				groups.size());
		final boolean groupFound[] = new boolean[2];
		for (final String grpId : groups) {
			final List<AnalyticItemWrapper<SimpleFeature>> items = centroidManager.getCentroidsForGroup(grpId);
			assertEquals(
					2,
					items.size());
			if ("grp1".equals(grpId)) {
				groupFound[0] = true;
				assertTrue("pearl".equals(items.get(
						0).getName()) || "roxy".equals(items.get(
						0).getName()));
			}
			else if ("grp2".equals(grpId)) {
				groupFound[1] = true;
				assertTrue("chip".equals(items.get(
						0).getName()) || "bamm-bamm".equals(items.get(
						0).getName()));
			}
		}
		// each unique group is found?
		int c = 0;
		for (final boolean gf : groupFound) {
			c += (gf ? 1 : 0);
		}
		assertEquals(
				2,
				c);
	}
}
