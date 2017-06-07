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
package mil.nga.giat.geowave.adapter.vector.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.text.ParseException;
import java.util.UUID;

import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.feature.SchemaException;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

import mil.nga.giat.geowave.adapter.vector.BaseDataStoreTest;
import mil.nga.giat.geowave.adapter.vector.utils.DateUtilities;

/**
 * Test with a default spatial index rather than geo-temporal
 * 
 */
public class WFSBoundedSpatialQueryTest extends
		BaseDataStoreTest
{
	DataStore dataStore;
	SimpleFeatureType schema;
	SimpleFeatureType type;
	final GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));

	@Before
	public void setup()
			throws SchemaException,
			CQLException,
			IOException,
			GeoWavePluginException {
		dataStore = createDataStore();
		type = DataUtilities.createType(
				"geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,pid:String,when:Date");

		type.getDescriptor(
				"when").getUserData().put(
				"time",
				false);
		dataStore.createSchema(type);

	}

	public void populate()
			throws IOException,
			CQLException,
			ParseException {
		final Transaction transaction1 = new DefaultTransaction();

		final FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriter(
				type.getTypeName(),
				transaction1);
		assertFalse(writer.hasNext());
		SimpleFeature newFeature = writer.next();
		newFeature.setAttribute(
				"pop",
				Long.valueOf(100));
		newFeature.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		newFeature.setAttribute(
				"when",
				DateUtilities.parseISO("2005-05-19T20:32:56Z"));
		newFeature.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						43.454,
						28.232)));
		writer.write();

		newFeature = writer.next();
		newFeature.setAttribute(
				"pop",
				Long.valueOf(100));
		newFeature.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		newFeature.setAttribute(
				"when",
				DateUtilities.parseISO("2005-05-18T20:32:56Z"));
		newFeature.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						43.454,
						27.232)));
		writer.write();

		newFeature = writer.next();
		newFeature.setAttribute(
				"pop",
				Long.valueOf(100));
		newFeature.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		newFeature.setAttribute(
				"when",
				DateUtilities.parseISO("2005-05-17T20:32:56Z"));
		newFeature.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						43.454,
						28.232)));
		writer.write();
		writer.close();
		transaction1.commit();
		transaction1.close();
	}

	@Test
	public void testGeo()
			throws CQLException,
			IOException,
			ParseException {

		populate();
		Transaction transaction2 = new DefaultTransaction();
		Query query = new Query(
				"geostuff",
				CQL.toFilter("BBOX(geometry,44,27,42,30) and when during 2005-05-01T20:32:56Z/2005-05-29T21:32:56Z"),
				new String[] {
					"geometry",
					"pid"
				});
		FeatureReader<SimpleFeatureType, SimpleFeature> reader = dataStore.getFeatureReader(
				query,
				transaction2);
		int c = 0;
		while (reader.hasNext()) {
			reader.next();
			c++;
		}
		reader.close();
		transaction2.commit();
		transaction2.close();
		assertEquals(
				3,
				c);

		transaction2 = new DefaultTransaction();
		query = new Query(
				"geostuff",
				CQL.toFilter("BBOX(geometry,42,28,44,30) and when during 2005-05-01T20:32:56Z/2005-05-29T21:32:56Z"),
				new String[] {
					"geometry",
					"pid"
				});
		reader = dataStore.getFeatureReader(
				query,
				transaction2);
		c = 0;
		while (reader.hasNext()) {
			reader.next();
			c++;
		}
		reader.close();
		transaction2.commit();
		transaction2.close();
		assertEquals(
				2,
				c);
	}

}
