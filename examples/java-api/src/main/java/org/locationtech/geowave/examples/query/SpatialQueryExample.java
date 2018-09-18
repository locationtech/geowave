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
package org.locationtech.geowave.examples.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**
 * This class is intended to provide a few examples on running Geowave queries
 * of different types: 1- Querying by polygon a set of points. 2- Filtering on
 * attributes of features using CQL queries 3- Ingesting polygons, and running
 * polygon intersect queries. You can check all points, geometries and query
 * accuracy in a more visual manner @ http://geojson.io/
 */
public class SpatialQueryExample
{
	private static Logger log = LoggerFactory.getLogger(SpatialQueryExample.class);

	private static DataStore dataStore;

	public static void main(
			final String[] args )
			throws AccumuloSecurityException,
			AccumuloException,
			ParseException,
			CQLException,
			IOException {
		final SpatialQueryExample example = new SpatialQueryExample();
		log.info("Setting up datastores");
		dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());
		log.info("Running point query examples");
		example.runPointExamples();
		log.info("Running polygon query examples");
		example.runPolygonExamples();
	}

	/**
	 * We'll run our point related operations. The data ingested and queried is
	 * single point based, meaning the index constructed will be based on a
	 * point.
	 */
	private void runPointExamples()
			throws ParseException,
			CQLException,
			IOException {
		ingestPointData();
		pointQuery();
	}

	private void ingestPointData() {
		log.info("Ingesting point data");
		ingestPointBasicFeature();
		ingestPointComplexFeature();
		log.info("Point data ingested");
	}

	private void ingest(
			final FeatureDataAdapter adapter,
			final Index index,
			final List<SimpleFeature> features ) {
		dataStore.addType(
				adapter,
				index);
		try (Writer<SimpleFeature> indexWriter = dataStore.createWriter(adapter.getTypeName())) {
			for (final SimpleFeature sf : features) {
				indexWriter.write(sf);

			}
		}
	}

	private void ingestPointBasicFeature() {
		// First, we'll build our first kind of SimpleFeature, which we'll call
		// "basic-feature"
		// We need the type builder to build the feature type
		final SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
		// AttributeTypeBuilder for the attributes of the SimpleFeature
		final AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();
		// Here we're setting the SimpleFeature name. Later on, we'll be able to
		// query GW just by this particular feature.
		sftBuilder.setName("basic-feature");
		// Add the attributes to the feature
		// Add the geometry attribute, which is mandatory for GeoWave to be able
		// to construct an index out of the SimpleFeature
		sftBuilder.add(attrBuilder.binding(
				Point.class).nillable(
				false).buildDescriptor(
				"geometry"));
		// Add another attribute just to be able to filter by it in CQL
		sftBuilder.add(attrBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"filter"));

		// Create the SimpleFeatureType
		final SimpleFeatureType sfType = sftBuilder.buildFeatureType();
		// We need the adapter for all our operations with GeoWave
		final FeatureDataAdapter sfAdapter = new FeatureDataAdapter(
				sfType);

		// Now we build the actual features. We'll create two points.
		// First point
		final SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(
				sfType);
		sfBuilder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						-80.211181640625,
						25.848101000701597)));
		sfBuilder.set(
				"filter",
				"Basic-Stadium");
		// When calling buildFeature, we need to pass an unique id for that
		// feature, or it will be overwritten.
		final SimpleFeature basicPoint1 = sfBuilder.buildFeature("1");

		// Construct the second feature.
		sfBuilder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						-80.191360,
						25.777804)));
		sfBuilder.set(
				"filter",
				"Basic-College");
		final SimpleFeature basicPoint2 = sfBuilder.buildFeature("2");

		final ArrayList<SimpleFeature> features = new ArrayList<>();
		features.add(basicPoint1);
		features.add(basicPoint2);

		// Ingest the data. For that purpose, we need the feature adapter,
		// the index type (the default spatial index is used here),
		// and an iterator of SimpleFeature
		ingest(
				sfAdapter,
				new SpatialIndexBuilder().createIndex(),
				features);
	}

	/**
	 * We're going to ingest a more complete simple feature.
	 */
	private void ingestPointComplexFeature() {
		// First, we'll build our second kind of SimpleFeature, which we'll call
		// "complex-feature"
		// We need the type builder to build the feature type
		final SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
		// AttributeTypeBuilder for the attributes of the SimpleFeature
		final AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();
		// Here we're setting the SimpleFeature name. Later on, we'll be able to
		// query GW just by this particular feature.
		sftBuilder.setName("complex-feature");
		// Add the attributes to the feature
		// Add the geometry attribute, which is mandatory for GeoWave to be able
		// to construct an index out of the SimpleFeature
		sftBuilder.add(attrBuilder.binding(
				Point.class).nillable(
				false).buildDescriptor(
				"geometry"));
		// Add another attribute just to be able to filter by it in CQL
		sftBuilder.add(attrBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"filter"));
		// Add more attributes to use with CQL filtering later on.
		sftBuilder.add(attrBuilder.binding(
				Double.class).nillable(
				false).buildDescriptor(
				"latitude"));
		sftBuilder.add(attrBuilder.binding(
				Double.class).nillable(
				false).buildDescriptor(
				"longitude"));

		// Create the SimpleFeatureType
		final SimpleFeatureType sfType = sftBuilder.buildFeatureType();
		// We need the adapter for all our operations with GeoWave
		final FeatureDataAdapter sfAdapter = new FeatureDataAdapter(
				sfType);

		// Now we build the actual features. We'll create two more points.
		// First point
		final SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(
				sfType);
		sfBuilder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						-80.193388,
						25.780538)));
		sfBuilder.set(
				"filter",
				"Complex-Station");
		sfBuilder.set(
				"latitude",
				25.780538);
		sfBuilder.set(
				"longitude",
				-80.193388);
		// When calling buildFeature, we need to pass an unique id for that
		// feature, or it will be overwritten.
		final SimpleFeature basicPoint1 = sfBuilder.buildFeature("1");

		// Construct the second feature.
		sfBuilder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						-118.26713562011719,
						33.988349152677955)));
		sfBuilder.set(
				"filter",
				"Complex-LA");
		sfBuilder.set(
				"latitude",
				33.988349152677955);
		sfBuilder.set(
				"longitude",
				-118.26713562011719);
		final SimpleFeature basicPoint2 = sfBuilder.buildFeature("2");

		final ArrayList<SimpleFeature> features = new ArrayList<>();
		features.add(basicPoint1);
		features.add(basicPoint2);

		// Ingest the data. For that purpose, we need the feature adapter,
		// the index type (the default spatial index is used here),
		// and an iterator of SimpleFeature
		ingest(
				sfAdapter,
				new SpatialIndexBuilder().createIndex(),
				features);

		/**
		 * After ingest, a single point might look like this in Accumulo.
		 */
		// \x1F\x11\xCB\xFC\xB6\xEFT\x00\xFFcomplex_feature4\x00\x00\x00\x0E\x00\x00\x00\x01\x00\x00\x00\x00
		// complex_feature:filter [] Complex-LA
		// \x1F\x11\xCB\xFC\xB6\xEFT\x00\xFFcomplex_feature4\x00\x00\x00\x0E\x00\x00\x00\x01\x00\x00\x00\x00
		// complex_feature:geom\x00\x00 []
		// \x00\x00\x00\x00\x01\xC0]\x91\x18\xC0\x00\x00\x00@@\xFE\x829\x9B\xE3\xFC
		// \x1F\x11\xCB\xFC\xB6\xEFT\x00\xFFcomplex_feature4\x00\x00\x00\x0E\x00\x00\x00\x01\x00\x00\x00\x00
		// complex_feature:latitude [] @@\xFE\x829\x9B\xE3\xFC
		// \x1F\x11\xCB\xFC\xB6\xEFT\x00\xFFcomplex_feature\x00\x00\x00\x0E\x00\x00\x00\x01\x00\x00\x00\x00
		// complex_feature:longitude [] \xC0]\x91\x18\xC0\x00\x00\x
	}

	/**
	 * This query will use a specific Bounding Box, and will find only 1 point.
	 */
	private void pointQuery()
			throws ParseException,
			IOException {
		log.info("Running Point Query Case 2");
		// First, we need to obtain the adapter for the SimpleFeature we want to
		// query.
		// We'll query complex-feature in this example.
		// Obtain adapter for our "complex-feature" type
		final String typeName = "complex-feature";

		// Define the geometry to query. We'll find all points that fall inside
		// that geometry.
		final String queryPolygonDefinition = "POLYGON (( " + "-118.50059509277344 33.75688594085081, "
				+ "-118.50059509277344 34.1521587488017, " + "-117.80502319335938 34.1521587488017, "
				+ "-117.80502319335938 33.75688594085081, " + "-118.50059509277344 33.75688594085081" + "))";

		final Geometry queryPolygon = new WKTReader(
				JTSFactoryFinder.getGeometryFactory()).read(queryPolygonDefinition);

		// Perform the query.Parameters are
		/**
		 * 1- Adapter previously obtained from the feature name. 2- Default
		 * spatial index. 3- A SpatialQuery, which takes the query geometry -
		 * aka Bounding box 4- Filters. For this example, no filter is used. 5-
		 * Limit. Same as standard SQL limit. 0 is no limits. 6- authorizations.
		 * For our example, "root" works. In a real , whatever authorization is
		 * associated to the user in question.
		 */

		int count = 0;

		final VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();
		try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(bldr.addTypeName(
				typeName).indexName(
				"SPATIAL_IDX").addAuthorization(
				"root").constraints(
				bldr.constraintsFactory().spatialTemporalConstraints().spatialConstraints(
						queryPolygon).build()).build())) {

			while (iterator.hasNext()) {
				final SimpleFeature sf = iterator.next();
				log.info("Obtained SimpleFeature " + sf.getName().toString() + " - " + sf.getAttribute("filter"));
				count++;
				System.out.println("Query match: " + sf.getID());
			}
			log.info("Should have obtained 1 feature. -> " + (count == 1));
		}
	}

	/**
	 * We'll run our polygon related operations. The data ingested and queried
	 * is single polygon based, meaning the index constructed will be based on a
	 * Geometry.
	 */
	private void runPolygonExamples()
			throws ParseException,
			IOException {
		ingestPolygonFeature();
		polygonQuery();
	}

	private void ingestPolygonFeature()
			throws ParseException {
		log.info("Ingesting polygon data");
		// First, we'll build our third kind of SimpleFeature, which we'll call
		// "polygon-feature"
		// We need the type builder to build the feature type
		final SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
		// AttributeTypeBuilder for the attributes of the SimpleFeature
		final AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();
		// Here we're setting the SimpleFeature name. Later on, we'll be able to
		// query GW just by this particular feature.
		sftBuilder.setName("polygon-feature");
		// Add the attributes to the feature
		// Add the geometry attribute, which is mandatory for GeoWave to be able
		// to construct an index out of the SimpleFeature
		// Will be any arbitrary geometry; in this case, a polygon.
		sftBuilder.add(attrBuilder.binding(
				Geometry.class).nillable(
				false).buildDescriptor(
				"geometry"));
		// Add another attribute just to be able to filter by it in CQL
		sftBuilder.add(attrBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"filter"));

		// Create the SimpleFeatureType
		final SimpleFeatureType sfType = sftBuilder.buildFeatureType();
		// We need the adapter for all our operations with GeoWave
		final FeatureDataAdapter sfAdapter = new FeatureDataAdapter(
				sfType);

		// Now we build the actual features. We'll create one polygon.
		// First point
		final SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(
				sfType);

		// For ease of use, we'll create the polygon geometry with WKT format.
		final String polygonDefinition = "POLYGON (( " + "-80.3045654296875 25.852426562716428, "
				+ "-80.123291015625 25.808545671771615, " + "-80.19195556640625 25.7244467526159, "
				+ "-80.34233093261719 25.772068899816585, " + "-80.3045654296875 25.852426562716428" + "))";
		final Geometry geom = new WKTReader(
				JTSFactoryFinder.getGeometryFactory()).read(polygonDefinition);
		sfBuilder.set(
				"geometry",
				geom);
		sfBuilder.set(
				"filter",
				"Polygon");
		// When calling buildFeature, we need to pass an unique id for that
		// feature, or it will be overwritten.
		final SimpleFeature polygon = sfBuilder.buildFeature("1");

		final ArrayList<SimpleFeature> features = new ArrayList<>();
		features.add(polygon);

		// Ingest the data. For that purpose, we need the feature adapter,
		// the index type (the default spatial index is used here),
		// and an iterator of SimpleFeature
		ingest(
				sfAdapter,
				new SpatialIndexBuilder().createIndex(),
				features);
		log.info("Polygon data ingested");
	}

	/**
	 * This query will find a polygon/polygon intersection, returning one match.
	 */
	private void polygonQuery()
			throws ParseException,
			IOException {
		log.info("Running Point Query Case 4");
		// First, we need to obtain the adapter for the SimpleFeature we want to
		// query.
		// We'll query polygon-feature in this example.
		// Obtain adapter for our "polygon-feature" type
		final String typeName = "polygon-feature";
		// Define the geometry to query. We'll find all polygons that intersect
		// with this geometry.
		final String queryPolygonDefinition = "POLYGON (( " + "-80.4037857055664 25.81596330265488, "
				+ "-80.27915954589844 25.788144792391982, " + "-80.34370422363281 25.8814655232439, "
				+ "-80.44567108154297 25.896291175546626, " + "-80.4037857055664  25.81596330265488" + "))";

		final Geometry queryPolygon = new WKTReader(
				JTSFactoryFinder.getGeometryFactory()).read(queryPolygonDefinition);

		// Perform the query.Parameters are
		/**
		 * 1- Adapter previously obtained from the feature name. 2- Default
		 * spatial index. 3- A SpatialQuery, which takes the query geometry -
		 * aka Bounding box 4- Filters. For this example, no filter is used. 5-
		 * Limit. Same as standard SQL limit. 0 is no limits. 6- authorizations.
		 * For our example, "root" works. In a real , whatever authorization is
		 * associated to the user in question.
		 */

		int count = 0;

		final VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();
		try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(bldr.addTypeName(
				typeName).indexName(
				"SPATIAL_IDX").addAuthorization(
				"root").constraints(
				bldr.constraintsFactory().spatialTemporalConstraints().spatialConstraints(
						queryPolygon).build()).build())) {

			while (iterator.hasNext()) {
				final SimpleFeature sf = iterator.next();
				log.info("Obtained SimpleFeature " + sf.getName().toString() + " - " + sf.getAttribute("filter"));
				count++;
				System.out.println("Query match: " + sf.getID());
			}
			log.info("Should have obtained 1 feature. -> " + (count == 1));
		}
	}
}
