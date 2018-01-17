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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

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
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;
import mil.nga.giat.geowave.adapter.vector.utils.DateUtilities;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalOptions;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter.CompareOperation;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialTemporalQuery;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraints;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalRange;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.minicluster.MiniAccumuloClusterFactory;

/**
 * This class is intended to provide a self-contained, easy-to-follow example of
 * a few GeoTools queries against GeoWave using Spatial Temporal Data.
 * 
 * For simplicity, a MiniAccumuloCluster is spun up and a few points from the DC
 * area are ingested (Washington Monument, White House, FedEx Field). Two
 * queries are executed against this data set.
 */
public class GeoTemporalQueryExample
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoTemporalQueryExample.class);

	private static File tempAccumuloDir;
	private static MiniAccumuloClusterImpl accumulo;
	private static AccumuloDataStore dataStore;

	private static final PrimaryIndex index = new SpatialTemporalDimensionalityTypeProvider()
			.createPrimaryIndex(new SpatialTemporalOptions());
	private static final FeatureDataAdapter adapter = new FeatureDataAdapter(
			getPointSimpleFeatureType());

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

	public GeoTemporalQueryExample() {}

	public static void main(
			final String[] args )
			throws AccumuloException,
			AccumuloSecurityException,
			InterruptedException,
			IOException,
			ParseException,
			TransformException {

		// spin up a MiniAccumuloCluster and initialize the DataStore
		setup();

		new GeoTemporalQueryExample().run();

		// stop MiniAccumuloCluster and delete temporary files
		cleanup();
	}

	public void run()
			throws AccumuloException,
			AccumuloSecurityException,
			InterruptedException,
			IOException,
			ParseException,
			TransformException {

		// ingest 3 points represented as SimpleFeatures: Washington Monument,
		// White House, FedEx Field
		ingestCannedData();

		// execute a query for a large polygon
		executePolygonAndTimeRangeQuery();
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
				GeoTemporalQueryExample.class);

		accumulo.start();

		dataStore = new AccumuloDataStore(
				new BasicAccumuloOperations(
						accumulo.getZooKeepers(),
						accumulo.getInstanceName(),
						ACCUMULO_USER,
						ACCUMULO_PASSWORD,
						TABLE_NAMESPACE));

	}

	private void ingestCannedData()
			throws IOException {

		final List<SimpleFeature> points = new ArrayList<>();

		System.out.println("Building SimpleFeatures from canned data set...");

		try {
			points.add(buildSimpleFeature(
					"Washington Monument 1",
					washingtonMonument,
					DateUtilities.parseISO("2005-05-15T20:32:56Z"),
					DateUtilities.parseISO("2005-05-15T21:32:56Z")));

			points.add(buildSimpleFeature(
					"Washington Monument 2",
					washingtonMonument,
					DateUtilities.parseISO("2005-05-17T20:32:56Z"),
					DateUtilities.parseISO("2005-05-17T21:32:56Z")));

			points.add(buildSimpleFeature(
					"White House 1",
					whiteHouse,
					DateUtilities.parseISO("2005-05-17T20:32:56Z"),
					DateUtilities.parseISO("2005-05-17T21:32:56Z")));

			points.add(buildSimpleFeature(
					"White House 2",
					whiteHouse,
					DateUtilities.parseISO("2005-05-17T19:32:56Z"),
					DateUtilities.parseISO("2005-05-17T20:45:56Z")));

			points.add(buildSimpleFeature(
					"Fedex 1",
					fedexField,
					DateUtilities.parseISO("2005-05-17T20:32:56Z"),
					DateUtilities.parseISO("2005-05-17T21:32:56Z")));

			points.add(buildSimpleFeature(
					"Fedex 2",
					fedexField,
					DateUtilities.parseISO("2005-05-18T19:32:56Z"),
					DateUtilities.parseISO("2005-05-18T20:45:56Z")));

			points.add(buildSimpleFeature(
					"White House 3",
					whiteHouse,
					DateUtilities.parseISO("2005-05-19T19:32:56Z"),
					DateUtilities.parseISO("2005-05-19T20:45:56Z")));

		}
		catch (final Exception ex) {
			LOGGER.warn(
					"Could not add points",
					ex);
		}

		System.out.println("Ingesting canned data...");

		try (IndexWriter indexWriter = dataStore.createWriter(
				adapter,
				index)) {
			for (final SimpleFeature sf : points) {
				//
				indexWriter.write(sf);

			}
		}

		System.out.println("Ingest complete.");
	}

	private void executePolygonAndTimeRangeQuery()
			throws IOException,
			ParseException,
			TransformException {

		// Query equivalent to ECQL:

		// DWITHIN(geometry, POINT(-77.03521 38.8895), 13.7, kilometers) and
		// startTime after 2005-05-17T19:32:56Z and endTime before
		// 2005-05-17T22:32:56Z
		//
		// Notice the use of CompareOperations.CONTAINS.
		// By default, SpatialTemporalQuery and SpatialTemporalQuery use
		// CompareOperations.OVERLAPS
		//
		// To compose the polygon, this query creates a characteristic 'circle'
		// around center given a distance.

		// The method Geometry.buffer() works in degrees; a helper
		// method is available that uses metric units. The helper method
		// looses accuracy as the distance from the centroid grows and
		// the centroid moves closer the poles.

		final SpatialTemporalQuery query = new SpatialTemporalQuery(
				DateUtilities.parseISO("2005-05-17T19:32:56Z"),
				DateUtilities.parseISO("2005-05-17T22:32:56Z"),
				mil.nga.giat.geowave.adapter.vector.utils.FeatureGeometryUtils.buffer(
						GeometryUtils.DEFAULT_CRS,
						GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								-77.03521,
								38.8895)),
						"meter",
						13700).getKey(),
				CompareOperation.CONTAINS);

		System.out.println("Executing query, expecting to match three points...");

		final CloseableIterator<SimpleFeature> iterator = dataStore.query(
				new QueryOptions(),
				query);

		while (iterator.hasNext()) {
			System.out.println("Query match: " + iterator.next().getID());
		}

		iterator.close();

		final TemporalConstraints tempotalIndexConstraints = new TemporalConstraints(
				Arrays.asList(
						new TemporalRange(
								DateUtilities.parseISO("2005-05-17T19:32:56Z"),
								DateUtilities.parseISO("2005-05-17T22:32:56Z")),
						new TemporalRange(
								DateUtilities.parseISO("2005-05-19T19:32:56Z"),
								DateUtilities.parseISO("2005-05-19T22:32:56Z"))),
				"ignored"); // the name is not used in this case

		final SpatialTemporalQuery query2 = new SpatialTemporalQuery(
				tempotalIndexConstraints,
				mil.nga.giat.geowave.adapter.vector.utils.FeatureGeometryUtils.buffer(
						GeometryUtils.DEFAULT_CRS,
						GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								-77.03521,
								38.8895)),
						"meter",
						13700).getKey(),
				CompareOperation.CONTAINS);

		System.out.println("Executing query # 2 with multiple time ranges, expecting to match four points...");

		final CloseableIterator<SimpleFeature> iterator2 = dataStore.query(
				new QueryOptions(
						adapter,
						index),
				query2);

		while (iterator2.hasNext()) {
			System.out.println("Query match: " + iterator2.next().getID());
		}

		iterator2.close();
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
		sftBuilder.add(atBuilder.binding(
				Date.class).nillable(
				false).buildDescriptor(
				"startTime"));
		sftBuilder.add(atBuilder.binding(
				Date.class).nillable(
				false).buildDescriptor(
				"endTime"));

		return sftBuilder.buildFeatureType();
	}

	private static SimpleFeature buildSimpleFeature(
			final String locationName,
			final Coordinate coordinate,
			final Date startTime,
			final Date endTime ) {

		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				getPointSimpleFeatureType());
		builder.set(
				"locationName",
				locationName);
		builder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate));
		builder.set(
				"startTime",
				startTime);
		builder.set(
				"endTime",
				endTime);

		return builder.buildFeature(locationName);
	}
}
