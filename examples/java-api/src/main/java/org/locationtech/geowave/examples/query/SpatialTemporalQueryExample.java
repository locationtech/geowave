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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.util.DateUtilities;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalOptions;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter.CompareOperation;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

/**
 * This class is intended to provide a self-contained, easy-to-follow example of
 * a few GeoTools queries against GeoWave using Spatial Temporal Data.
 *
 * For simplicity, a MiniAccumuloCluster is spun up and a few points from the DC
 * area are ingested (Washington Monument, White House, FedEx Field). Two
 * queries are executed against this data set.
 */
public class SpatialTemporalQueryExample
{
	private static final Logger LOGGER = LoggerFactory.getLogger(SpatialTemporalQueryExample.class);

	private DataStore dataStore;

	private static final Index index = new SpatialTemporalDimensionalityTypeProvider()
			.createIndex(new SpatialTemporalOptions());
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

	public SpatialTemporalQueryExample() {}

	public static void main(
			final String[] args )
			throws AccumuloException,
			AccumuloSecurityException,
			InterruptedException,
			IOException,
			ParseException,
			TransformException {
		new SpatialTemporalQueryExample().run();
	}

	public void run()
			throws AccumuloException,
			AccumuloSecurityException,
			InterruptedException,
			IOException,
			ParseException,
			TransformException {

		dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());
		// ingest 3 points represented as SimpleFeatures: Washington Monument,
		// White House, FedEx Field
		ingestCannedData();

		// execute a query for a large polygon
		executePolygonAndTimeRangeQuery();
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
		dataStore.addType(
				adapter,
				index);
		try (Writer<SimpleFeature> indexWriter = dataStore.createWriter(adapter.getTypeName())) {
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

		System.out.println("Executing query, expecting to match three points...");
		VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();
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
		final CloseableIterator<SimpleFeature> iterator = dataStore.query(bldr.constraints(
				bldr.constraintsFactory().spatialTemporalConstraints().addTimeRange(
						DateUtilities.parseISO("2005-05-17T19:32:56Z"),
						DateUtilities.parseISO("2005-05-17T22:32:56Z")).spatialConstraints(
						GeometryUtils.buffer(
								GeometryUtils.getDefaultCRS(),
								GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
										-77.03521,
										38.8895)),
								"meter",
								13700).getKey()).spatialConstraintsCompareOperation(
						CompareOperation.CONTAINS).build()).build());

		while (iterator.hasNext()) {
			System.out.println("Query match: " + iterator.next().getID());
		}

		iterator.close();

		System.out.println("Executing query # 2 with multiple time ranges, expecting to match four points...");
		bldr = VectorQueryBuilder.newBuilder();
		final CloseableIterator<SimpleFeature> iterator2 = dataStore.query(bldr.addTypeName(
				adapter.getTypeName()).indexName(
				index.getName()).constraints(
				bldr.constraintsFactory().spatialTemporalConstraints().addTimeRange(
						DateUtilities.parseISO("2005-05-17T19:32:56Z"),
						DateUtilities.parseISO("2005-05-17T22:32:56Z")).addTimeRange(
						DateUtilities.parseISO("2005-05-19T19:32:56Z"),
						DateUtilities.parseISO("2005-05-19T22:32:56Z")).spatialConstraints(
						GeometryUtils.buffer(
								GeometryUtils.getDefaultCRS(),
								GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
										-77.03521,
										38.8895)),
								"meter",
								13700).getKey()).spatialConstraintsCompareOperation(
						CompareOperation.CONTAINS).build()).build());

		while (iterator2.hasNext()) {
			System.out.println("Query match: " + iterator2.next().getID());
		}

		iterator2.close();
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
