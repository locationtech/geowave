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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.analytic.AnalyticFeature;
import org.locationtech.geowave.analytic.SimpleFeatureItemWrapperFactory;
import org.locationtech.geowave.analytic.distance.FeatureCentroidDistanceFn;
import org.locationtech.geowave.analytic.kmeans.AssociationNotification;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class NestedGroupCentroidAssignmentTest
{

	@Rule
	public TestName name = new TestName();

	private <T> void ingest(
			final DataStore dataStore,
			final DataTypeAdapter<T> adapter,
			final Index index,
			final T entry )
			throws IOException {
		dataStore.addType(
				adapter,
				index);
		try (Writer writer = dataStore.createWriter(adapter.getTypeName())) {
			writer.write(entry);
		}
	}

	@Test
	public void test()
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

		final SimpleFeature level1b1G1Feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"level1b1G1Feature",
				"fred",
				grp1,
				20.30203,
				factory.createPoint(new Coordinate(
						02.5,
						0.25)),
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
		final DataStorePluginOptions storePluginOptions = new DataStorePluginOptions(
				opts);
		final DataStore dataStore = storeFamily.getDataStoreFactory().createStore(
				opts);
		final IndexStore indexStore = storeFamily.getIndexStoreFactory().createStore(
				opts);
		final PersistentAdapterStore adapterStore = storeFamily.getAdapterStoreFactory().createStore(
				opts);

		ingest(
				dataStore,
				adapter,
				index,
				level1b1G1Feature);

		final SimpleFeature level1b1G2Feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"level1b1G2Feature",
				"flood",
				grp2,
				20.30203,
				factory.createPoint(new Coordinate(
						02.03,
						0.2)),
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
				level1b1G2Feature);

		final SimpleFeature level2b1G1Feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"level2b1G1Feature",
				"flou",
				level1b1G1Feature.getID(),
				20.30203,
				factory.createPoint(new Coordinate(
						02.5,
						0.25)),
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
				level2b1G1Feature);

		final SimpleFeature level2b1G2Feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"level2b1G2Feature",
				"flapper",
				level1b1G2Feature.getID(),
				20.30203,
				factory.createPoint(new Coordinate(
						02.03,
						0.2)),
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
				level2b1G2Feature);

		// different batch
		final SimpleFeature level2B2G1Feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b2",
				"level2B2G1Feature",
				"flapper",
				level1b1G1Feature.getID(),
				20.30203,
				factory.createPoint(new Coordinate(
						02.63,
						0.25)),
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
				level2B2G1Feature);

		final SimpleFeatureItemWrapperFactory wrapperFactory = new SimpleFeatureItemWrapperFactory();
		final CentroidManagerGeoWave<SimpleFeature> mananger = new CentroidManagerGeoWave<>(
				dataStore,
				indexStore,
				adapterStore,
				new SimpleFeatureItemWrapperFactory(),
				adapter.getTypeName(),
				storePluginOptions.createInternalAdapterStore().getAdapterId(
						adapter.getTypeName()),
				index.getName(),
				"b1",
				1);

		final List<CentroidPairing<SimpleFeature>> capturedPairing = new ArrayList<>();
		final AssociationNotification<SimpleFeature> assoc = new AssociationNotification<SimpleFeature>() {
			@Override
			public void notify(
					final CentroidPairing<SimpleFeature> pairing ) {
				capturedPairing.add(pairing);
			}
		};

		final FeatureCentroidDistanceFn distanceFn = new FeatureCentroidDistanceFn();
		final NestedGroupCentroidAssignment<SimpleFeature> assigmentB1 = new NestedGroupCentroidAssignment<>(
				mananger,
				1,
				"b1",
				distanceFn);
		assigmentB1.findCentroidForLevel(
				wrapperFactory.create(level1b1G1Feature),
				assoc);
		assertEquals(
				1,
				capturedPairing.size());
		assertEquals(
				level1b1G1Feature.getID(),
				capturedPairing.get(
						0).getCentroid().getID());
		capturedPairing.clear();

		final NestedGroupCentroidAssignment<SimpleFeature> assigmentB1L2G1 = new NestedGroupCentroidAssignment<>(
				mananger,
				2,
				"b1",
				distanceFn);
		assigmentB1L2G1.findCentroidForLevel(
				wrapperFactory.create(level1b1G1Feature),
				assoc);
		assertEquals(
				1,
				capturedPairing.size());
		assertEquals(
				level2b1G1Feature.getID(),
				capturedPairing.get(
						0).getCentroid().getID());
		capturedPairing.clear();

		// level 2 and different parent grouping
		final NestedGroupCentroidAssignment<SimpleFeature> assigmentB1L2G2 = new NestedGroupCentroidAssignment<>(
				mananger,
				2,
				"b1",
				distanceFn);
		assigmentB1L2G2.findCentroidForLevel(
				wrapperFactory.create(level1b1G2Feature),
				assoc);
		assertEquals(
				1,
				capturedPairing.size());
		assertEquals(
				level2b1G2Feature.getID(),
				capturedPairing.get(
						0).getCentroid().getID());
		capturedPairing.clear();

		// level two with different batch than parent

		final CentroidManagerGeoWave<SimpleFeature> mananger2 = new CentroidManagerGeoWave<>(
				dataStore,
				indexStore,
				adapterStore,
				new SimpleFeatureItemWrapperFactory(),
				adapter.getTypeName(),
				storePluginOptions.createInternalAdapterStore().getAdapterId(
						adapter.getTypeName()),

				index.getName(),
				"b2",
				2);
		final NestedGroupCentroidAssignment<SimpleFeature> assigmentB2L2 = new NestedGroupCentroidAssignment<>(
				mananger2,
				2,
				"b1",
				distanceFn);

		assigmentB2L2.findCentroidForLevel(
				wrapperFactory.create(level1b1G1Feature),
				assoc);
		assertEquals(
				1,
				capturedPairing.size());
		assertEquals(
				level2B2G1Feature.getID(),
				capturedPairing.get(
						0).getCentroid().getID());
		capturedPairing.clear();

	}
}
