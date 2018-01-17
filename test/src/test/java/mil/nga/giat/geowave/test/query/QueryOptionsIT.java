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
package mil.nga.giat.geowave.test.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialOptions;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
public class QueryOptionsIT
{
	private static SimpleFeatureType type1;
	private static SimpleFeatureType type2;
	private static FeatureDataAdapter dataAdapter1;
	private static FeatureDataAdapter dataAdapter2;
	// constants for attributes of SimpleFeatureType
	private static final String CITY_ATTRIBUTE = "city";
	private static final String STATE_ATTRIBUTE = "state";
	private static final String POPULATION_ATTRIBUTE = "population";
	private static final String LAND_AREA_ATTRIBUTE = "landArea";
	private static final String GEOMETRY_ATTRIBUTE = "geometry";

	// points used to construct bounding box for queries
	private static final Coordinate GUADALAJARA = new Coordinate(
			-103.3500,
			20.6667);
	private static final Coordinate ATLANTA = new Coordinate(
			-84.3900,
			33.7550);

	private final Query spatialQuery = new SpatialQuery(
			GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(
					GUADALAJARA,
					ATLANTA)));
	private static PrimaryIndex index = new SpatialDimensionalityTypeProvider()
			.createPrimaryIndex(new SpatialOptions());

	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.HBASE,
		GeoWaveStoreType.BIGTABLE
	})
	protected DataStorePluginOptions dataStoreOptions;

	@BeforeClass
	public static void setupData()
			throws IOException {
		type1 = getSimpleFeatureType("type1");
		type2 = getSimpleFeatureType("type2");
		dataAdapter1 = new FeatureDataAdapter(
				type1);
		dataAdapter1.init(index);
		dataAdapter2 = new FeatureDataAdapter(
				type2);
		dataAdapter2.init(index);
	}

	@Before
	public void ingestData()
			throws IOException {
		TestUtils.deleteAll(dataStoreOptions);
		ingestSampleData(
				new SimpleFeatureBuilder(
						type1),
				dataAdapter1);
		ingestSampleData(
				new SimpleFeatureBuilder(
						type2),
				dataAdapter2);
	}

	@Test
	public void testQuerySpecificAdapter()
			throws IOException {
		int numResults = 0;
		try (final CloseableIterator<SimpleFeature> results = dataStoreOptions.createDataStore().query(
				new QueryOptions(
						dataAdapter1,
						TestUtils.DEFAULT_SPATIAL_INDEX),
				spatialQuery)) {
			while (results.hasNext()) {
				numResults++;
				final SimpleFeature currFeat = results.next();
				Assert.assertTrue(
						"Expected state to be 'Texas'",
						currFeat.getAttribute(
								STATE_ATTRIBUTE).equals(
								"Texas"));
			}
		}
		Assert.assertTrue(
				"Expected 3 results but returned " + numResults,
				3 == numResults);
	}

	@Test
	public void testQueryAcrossAdapters()
			throws IOException {
		int numResults = 0;
		try (final CloseableIterator<SimpleFeature> results = dataStoreOptions.createDataStore().query(
				new QueryOptions(
						TestUtils.DEFAULT_SPATIAL_INDEX),
				spatialQuery)) {
			while (results.hasNext()) {
				numResults++;
				final SimpleFeature currFeat = results.next();
				Assert.assertTrue(
						"Expected state to be 'Texas'",
						currFeat.getAttribute(
								STATE_ATTRIBUTE).equals(
								"Texas"));
			}
		}
		Assert.assertTrue(
				"Expected 6 results but returned " + numResults,
				6 == numResults);
	}

	@Test
	public void testQueryEmptyOptions()
			throws IOException {
		int numResults = 0;
		try (final CloseableIterator<SimpleFeature> results = dataStoreOptions.createDataStore().query(
				new QueryOptions(),
				spatialQuery)) {
			while (results.hasNext()) {
				numResults++;
				final SimpleFeature currFeat = results.next();
				Assert.assertTrue(
						"Expected state to be 'Texas'",
						currFeat.getAttribute(
								STATE_ATTRIBUTE).equals(
								"Texas"));
			}
		}
		Assert.assertTrue(
				"Expected 6 results but returned " + numResults,
				6 == numResults);
	}

	private static SimpleFeatureType getSimpleFeatureType(
			final String typeName ) {
		SimpleFeatureType type = null;
		try {
			type = DataUtilities.createType(
					typeName,
					CITY_ATTRIBUTE + ":String," + STATE_ATTRIBUTE + ":String," + POPULATION_ATTRIBUTE + ":Double,"
							+ LAND_AREA_ATTRIBUTE + ":Double," + GEOMETRY_ATTRIBUTE + ":Geometry");
		}
		catch (final SchemaException e) {
			System.out.println("Unable to create SimpleFeatureType");
		}
		return type;
	}

	@SuppressWarnings("unchecked")
	private void ingestSampleData(
			final SimpleFeatureBuilder builder,
			final DataAdapter<?> adapter )
			throws IOException {
		try (@SuppressWarnings("rawtypes")
		IndexWriter writer = dataStoreOptions.createDataStore().createWriter(
				adapter,
				TestUtils.DEFAULT_SPATIAL_INDEX)) {
			for (final SimpleFeature sf : buildCityDataSet(builder)) {
				writer.write(sf);
			}
		}
	}

	private static List<SimpleFeature> buildCityDataSet(
			final SimpleFeatureBuilder builder ) {
		final List<SimpleFeature> points = new ArrayList<>();
		// http://en.wikipedia.org/wiki/List_of_United_States_cities_by_population
		points.add(buildSimpleFeature(
				builder,
				"New York",
				"New York",
				8405837,
				302.6,
				new Coordinate(
						-73.9385,
						40.6643)));
		points.add(buildSimpleFeature(
				builder,
				"Los Angeles",
				"California",
				3884307,
				468.7,
				new Coordinate(
						-118.4108,
						34.0194)));
		points.add(buildSimpleFeature(
				builder,
				"Chicago",
				"Illinois",
				2718782,
				227.6,
				new Coordinate(
						-87.6818,
						41.8376)));
		points.add(buildSimpleFeature(
				builder,
				"Houston",
				"Texas",
				2195914,
				599.6,
				new Coordinate(
						-95.3863,
						29.7805)));
		points.add(buildSimpleFeature(
				builder,
				"Philadelphia",
				"Pennsylvania",
				1553165,
				134.1,
				new Coordinate(
						-75.1333,
						40.0094)));
		points.add(buildSimpleFeature(
				builder,
				"Phoenix",
				"Arizona",
				1513367,
				516.7,
				new Coordinate(
						-112.088,
						33.5722)));
		points.add(buildSimpleFeature(
				builder,
				"San Antonio",
				"Texas",
				1409019,
				460.9,
				new Coordinate(
						-98.5251,
						29.4724)));
		points.add(buildSimpleFeature(
				builder,
				"San Diego",
				"California",
				1355896,
				325.2,
				new Coordinate(
						-117.135,
						32.8153)));
		points.add(buildSimpleFeature(
				builder,
				"Dallas",
				"Texas",
				1257676,
				340.5,
				new Coordinate(
						-96.7967,
						32.7757)));
		points.add(buildSimpleFeature(
				builder,
				"San Jose",
				"California",
				998537,
				176.5,
				new Coordinate(
						-121.8193,
						37.2969)));
		return points;
	}

	private static SimpleFeature buildSimpleFeature(
			final SimpleFeatureBuilder builder,
			final String city,
			final String state,
			final double population,
			final double landArea,
			final Coordinate coordinate ) {
		builder.set(
				CITY_ATTRIBUTE,
				city);
		builder.set(
				STATE_ATTRIBUTE,
				state);
		builder.set(
				POPULATION_ATTRIBUTE,
				population);
		builder.set(
				LAND_AREA_ATTRIBUTE,
				landArea);
		builder.set(
				GEOMETRY_ATTRIBUTE,
				GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate));
		return builder.buildFeature(UUID.randomUUID().toString());
	}
}
