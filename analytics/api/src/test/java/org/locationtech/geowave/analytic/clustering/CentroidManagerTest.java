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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.analytic.AnalyticFeature;
import org.locationtech.geowave.analytic.AnalyticItemWrapper;
import org.locationtech.geowave.analytic.SimpleFeatureItemWrapperFactory;
import org.locationtech.geowave.analytic.clustering.CentroidManager.CentroidProcessingFn;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class CentroidManagerTest
{
	@Rule
	public TestName name = new TestName();

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
		}
	}

	@Test
	public void testSampleRecall()
			throws IOException {

		final SimpleFeatureType ftype = AnalyticFeature.createGeometryFeatureAdapter(
				"centroid",
				new String[] {
					"extra1"
				},
				BasicFeatureTypes.DEFAULT_NAMESPACE,
				ClusteringUtils.CLUSTERING_CRS).getFeatureType();
		final GeometryFactory factory = new GeometryFactory();
		final String grp1 = "g1";
		final String grp2 = "g2";
		SimpleFeature feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
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

		final Index index = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
		final FeatureDataAdapter adapter = new FeatureDataAdapter(
				ftype);
		adapter.init(index);
		final String namespace = "test_" + getClass().getName() + "_" + name.getMethodName();
		final StoreFactoryFamilySpi storeFamily = new MemoryStoreFactoryFamily();
		final StoreFactoryOptions opts = storeFamily.getDataStoreFactory().createOptionsInstance();
		opts.setGeowaveNamespace(namespace);
		final DataStore dataStore = storeFamily.getDataStoreFactory().createStore(
				opts);
		final IndexStore indexStore = storeFamily.getIndexStoreFactory().createStore(
				opts);
		final PersistentAdapterStore adapterStore = storeFamily.getAdapterStoreFactory().createStore(
				opts);
		final InternalAdapterStore internalAdapterStore = storeFamily.getInternalAdapterStoreFactory().createStore(
				opts);
		ingest(
				dataStore,

				adapter,
				index,
				feature);

		feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"231",
				"flood",
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
		ingest(
				dataStore,
				adapter,
				index,
				feature);

		feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"321",
				"flou",
				grp2,
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
		ingest(
				dataStore,
				adapter,
				index,
				feature);

		feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b2",
				"312",
				"flapper",
				grp2,
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
		ingest(
				dataStore,
				adapter,
				index,
				feature);

		// and one feature with a different zoom level
		feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b2",
				"312",
				"flapper",
				grp2,
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
				2,
				1,
				0);
		ingest(
				dataStore,
				adapter,
				index,
				feature);

		CentroidManagerGeoWave<SimpleFeature> manager = new CentroidManagerGeoWave<>(
				dataStore,
				indexStore,
				adapterStore,
				new SimpleFeatureItemWrapperFactory(),
				adapter.getTypeName(),
				internalAdapterStore.getAdapterId(adapter.getTypeName()),
				index.getName(),
				"b1",
				1);
		List<AnalyticItemWrapper<SimpleFeature>> centroids = manager.getCentroidsForGroup(null);

		assertEquals(
				3,
				centroids.size());
		feature = centroids.get(
				0).getWrappedItem();
		assertEquals(
				0.022,
				(Double) feature.getAttribute("extra1"),
				0.001);

		centroids = manager.getCentroidsForGroup(grp1);
		assertEquals(
				2,
				centroids.size());
		centroids = manager.getCentroidsForGroup(grp2);
		assertEquals(
				1,
				centroids.size());
		feature = centroids.get(
				0).getWrappedItem();
		assertEquals(
				0.022,
				(Double) feature.getAttribute("extra1"),
				0.001);

		manager = new CentroidManagerGeoWave<>(
				dataStore,
				indexStore,
				adapterStore,
				new SimpleFeatureItemWrapperFactory(),
				adapter.getTypeName(),
				internalAdapterStore.getAdapterId(adapter.getTypeName()),
				index.getName(),
				"b1",
				1);

		manager.processForAllGroups(new CentroidProcessingFn<SimpleFeature>() {

			@Override
			public int processGroup(
					final String groupID,
					final List<AnalyticItemWrapper<SimpleFeature>> centroids ) {
				if (groupID.equals(grp1)) {
					assertEquals(
							2,
							centroids.size());
				}
				else if (groupID.equals(grp2)) {
					assertEquals(
							1,
							centroids.size());
				}
				else {
					assertTrue(
							"what group is this : " + groupID,
							false);
				}
				return 0;
			}

		});

	}
}
