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
package mil.nga.giat.geowave.examples.ingest;

import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.InternalAdapterStoreImpl;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.operations.AccumuloOperations;

public class SimpleIngestTest
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SimpleIngestTest.class);

	final AccumuloOptions accumuloOptions = new AccumuloOptions();
	final GeometryFactory factory = new GeometryFactory();
	final String AccumuloUser = "root";
	final PasswordToken AccumuloPass = new PasswordToken(
			new byte[0]);
	AccumuloOperations accumuloOperations;
	IndexStore indexStore;
	PersistentAdapterStore adapterStore;
	DataStatisticsStore statsStore;
	AccumuloDataStore mockDataStore;

	@Before
	public void setUp() {
		final MockInstance mockInstance = new MockInstance();
		Connector mockConnector = null;
		try {
			mockConnector = mockInstance.getConnector(
					AccumuloUser,
					AccumuloPass);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Failed to create mock accumulo connection",
					e);
		}
		accumuloOperations = new AccumuloOperations(
				mockConnector,
				accumuloOptions);

		indexStore = new IndexStoreImpl(
				accumuloOperations,
				accumuloOptions);

		adapterStore = new AdapterStoreImpl(
				accumuloOperations,
				accumuloOptions);

		statsStore = new DataStatisticsStoreImpl(
				accumuloOperations,
				accumuloOptions);

		mockDataStore = new AccumuloDataStore(
				indexStore,
				adapterStore,
				statsStore,
				new AccumuloSecondaryIndexDataStore(
						accumuloOperations),
				new AdapterIndexMappingStoreImpl(
						accumuloOperations,
						accumuloOptions),
				accumuloOperations,
				accumuloOptions,
				new InternalAdapterStoreImpl(
						accumuloOperations));

		accumuloOptions.setCreateTable(true);
		accumuloOptions.setUseAltIndex(true);
		accumuloOptions.setPersistDataStatistics(true);
	}

	protected static Set<Point> getCalcedPointSet() {
		final Set<Point> calcPoints = new TreeSet<Point>();
		for (int longitude = -180; longitude <= 180; longitude += 5) {
			for (int latitude = -90; latitude <= 90; latitude += 5) {
				final Point p = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						longitude,
						latitude));
				calcPoints.add(p);
			}
		}
		return calcPoints;
	}

	protected static Set<Point> getStoredPointSet(
			final DataStore ds ) {
		final CloseableIterator itr = ds.query(
				new QueryOptions(),
				new BasicQuery(
						new BasicQuery.Constraints()));
		final Set<Point> readPoints = new TreeSet<Point>();
		while (itr.hasNext()) {
			final Object n = itr.next();
			if (n instanceof SimpleFeature) {
				final SimpleFeature gridCell = (SimpleFeature) n;
				final Point p = (Point) gridCell.getDefaultGeometry();
				readPoints.add(p);
			}
		}
		return readPoints;
	}

	protected static void validate(
			final DataStore ds ) {
		final Set<Point> readPoints = getStoredPointSet(ds);
		final Set<Point> calcPoints = getCalcedPointSet();

		Assert.assertTrue(readPoints.equals(calcPoints));
	}

}
