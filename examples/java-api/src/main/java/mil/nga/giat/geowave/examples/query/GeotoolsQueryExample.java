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
package mil.nga.giat.geowave.examples.query;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.commons.io.FileUtils;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialOptions;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.minicluster.MiniAccumuloClusterFactory;

/**
 * This class is intended to provide a self-contained, easy-to-follow example of
 * a few GeoTools queries against GeoWave. For simplicity, a MiniAccumuloCluster
 * is spun up and a few points from the DC area are ingested (Washington
 * Monument, White House, FedEx Field). Two queries are executed against this
 * data set.
 */
public class GeotoolsQueryExample
{
	private static File tempAccumuloDir;
	private static MiniAccumuloClusterImpl accumulo;
	private static DataStore dataStore;

	private static final PrimaryIndex index = new SpatialDimensionalityTypeProvider()
			.createPrimaryIndex(new SpatialOptions());

	// Points (to be ingested into GeoWave Data Store)
	private static final Coordinate washingtonMonument = new Coordinate(
			-77.0352,
			38.8895);
	private static final Coordinate whiteHouse = new Coordinate(
			-77.0366,
			38.8977);
	private static final Coordinate fedexField = new Coordinate(
			-76.8644,
			38.9078);

	// cities used to construct Geometries for queries
	private static final Coordinate baltimore = new Coordinate(
			-76.6167,
			39.2833);
	private static final Coordinate richmond = new Coordinate(
			-77.4667,
			37.5333);
	private static final Coordinate harrisonburg = new Coordinate(
			-78.8689,
			38.4496);

	private static final Map<String, Coordinate> cannedData = new HashMap<>();

	static {
		cannedData.put(
				"Washington Monument",
				washingtonMonument);
		cannedData.put(
				"White House",
				whiteHouse);
		cannedData.put(
				"FedEx Field",
				fedexField);
	}

	final static FeatureDataAdapter ADAPTER = new FeatureDataAdapter(
			getPointSimpleFeatureType());

	public static void main(
			final String[] args )
			throws AccumuloException,
			AccumuloSecurityException,
			InterruptedException,
			IOException {

		// spin up a MiniAccumuloCluster and initialize the DataStore
		setup();

		// ingest 3 points represented as SimpleFeatures: Washington Monument,
		// White House, FedEx Field
		ingestCannedData();

		// execute a query for a bounding box
		executeBoundingBoxQuery();

		// execute a query for a large polygon
		executePolygonQuery();

		// stop MiniAccumuloCluster and delete temporary files
		cleanup();
	}

	private static void setup()
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			InterruptedException {

		final String ACCUMULO_USER = "root";
		final String ACCUMULO_PASSWORD = "Ge0wave";
		final String TABLE_NAMESPACE = "";

		tempAccumuloDir = Files.createTempDir();

		accumulo = MiniAccumuloClusterFactory.newAccumuloCluster(
				new MiniAccumuloConfigImpl(
						tempAccumuloDir,
						ACCUMULO_PASSWORD),
				GeotoolsQueryExample.class);

		accumulo.start();

		dataStore = new AccumuloDataStore(
				new BasicAccumuloOperations(
						accumulo.getZooKeepers(),
						accumulo.getInstanceName(),
						ACCUMULO_USER,
						ACCUMULO_PASSWORD,
						TABLE_NAMESPACE));
	}

	private static void ingestCannedData()
			throws IOException {

		final List<SimpleFeature> points = new ArrayList<>();

		System.out.println("Building SimpleFeatures from canned data set...");

		for (final Entry<String, Coordinate> entry : cannedData.entrySet()) {
			System.out.println("Added point: " + entry.getKey());
			points.add(buildSimpleFeature(
					entry.getKey(),
					entry.getValue()));
		}

		System.out.println("Ingesting canned data...");

		try (IndexWriter indexWriter = dataStore.createWriter(
				ADAPTER,
				index)) {
			for (final SimpleFeature sf : points) {
				//
				indexWriter.write(sf);

			}
		}

		System.out.println("Ingest complete.");
	}

	private static void executeBoundingBoxQuery()
			throws IOException {

		System.out.println("Constructing bounding box for the area contained by [Baltimore, MD and Richmond, VA.");

		final Geometry boundingBox = GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(
				baltimore,
				richmond));

		System.out.println("Executing query, expecting to match ALL points...");

		try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(
				new QueryOptions(
						ADAPTER,
						index),
				new SpatialQuery(
						boundingBox))) {

			while (iterator.hasNext()) {
				System.out.println("Query match: " + iterator.next().getID());
			}
		}

	}

	private static void executePolygonQuery()
			throws IOException {

		System.out
				.println("Constructing polygon for the area contained by [Baltimore, MD; Richmond, VA; Harrisonburg, VA].");

		final Polygon polygon = GeometryUtils.GEOMETRY_FACTORY.createPolygon(new Coordinate[] {
			baltimore,
			richmond,
			harrisonburg,
			baltimore
		});

		System.out.println("Executing query, expecting to match ALL points...");

		/**
		 * NOTICE: In this query, the adapter is added to the query options. If
		 * an index has data from more than one adapter, the data associated
		 * with a specific adapter can be selected.
		 */
		try (final CloseableIterator<SimpleFeature> closableIterator = dataStore.query(
				new QueryOptions(
						ADAPTER,
						index),
				new SpatialQuery(
						polygon))) {

			while (closableIterator.hasNext()) {
				System.out.println("Query match: " + closableIterator.next().getID());
			}
		}

	}

	private static void cleanup()
			throws IOException,
			InterruptedException {

		try {
			accumulo.stop();
		}
		finally {
			FileUtils.deleteDirectory(tempAccumuloDir);
		}
	}

	private static SimpleFeatureType getPointSimpleFeatureType() {

		final String NAME = "PointSimpleFeatureType";
		final SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
		final AttributeTypeBuilder atBuilder = new AttributeTypeBuilder();
		sftBuilder.setName(NAME);
		sftBuilder.add(atBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"locationName"));
		sftBuilder.add(atBuilder.binding(
				Geometry.class).nillable(
				false).buildDescriptor(
				"geometry"));

		return sftBuilder.buildFeatureType();
	}

	private static SimpleFeature buildSimpleFeature(
			final String locationName,
			final Coordinate coordinate ) {

		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				getPointSimpleFeatureType());
		builder.set(
				"locationName",
				locationName);
		builder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate));

		return builder.buildFeature(locationName);
	}

}
