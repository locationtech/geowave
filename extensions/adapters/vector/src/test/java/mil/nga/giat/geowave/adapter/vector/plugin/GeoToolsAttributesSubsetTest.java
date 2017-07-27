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

import static org.junit.Assert.assertFalse;

import java.io.IOException;

import mil.nga.giat.geowave.adapter.vector.BaseDataStoreTest;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;

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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;

public class GeoToolsAttributesSubsetTest extends
		BaseDataStoreTest
{
	private DataStore geotoolsDataStore;
	private SimpleFeatureType type;
	private static final String typeName = "testStuff";
	private static final String typeSpec = "geometry:Geometry:srid=4326,aLong:java.lang.Long,aString:String";
	private static final String cqlPredicate = "BBOX(geometry,40,40,42,42)";
	private static final String geometry_attribute = "geometry";
	private static final String long_attribute = "aLong";
	private static final String string_attribute = "aString";

	@Before
	public void setup()
			throws IOException,
			GeoWavePluginException,
			SchemaException {
		geotoolsDataStore = createDataStore();
		type = DataUtilities.createType(
				typeName,
				typeSpec);

		geotoolsDataStore.createSchema(type);
		final Transaction transaction = new DefaultTransaction();
		final FeatureWriter<SimpleFeatureType, SimpleFeature> writer = geotoolsDataStore.getFeatureWriter(
				type.getTypeName(),
				transaction);
		assertFalse(writer.hasNext());
		SimpleFeature newFeature = writer.next();
		newFeature.setAttribute(
				geometry_attribute,
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						41.25,
						41.25)));
		newFeature.setAttribute(
				long_attribute,
				1l);
		newFeature.setAttribute(
				string_attribute,
				"string1");
		writer.write();
		newFeature = writer.next();
		newFeature.setAttribute(
				geometry_attribute,
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						41.5,
						41.5)));
		newFeature.setAttribute(
				long_attribute,
				2l);
		newFeature.setAttribute(
				string_attribute,
				"string2");
		writer.write();
		newFeature = writer.next();
		newFeature.setAttribute(
				geometry_attribute,
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						41.75,
						41.75)));
		newFeature.setAttribute(
				long_attribute,
				3l);
		newFeature.setAttribute(
				string_attribute,
				"string3");
		writer.write();
		writer.close();
		transaction.commit();
		transaction.close();
	}

	@Test
	public void testAllAttributes()
			throws CQLException,
			IOException {
		final Query query = new Query(
				typeName,
				CQL.toFilter(cqlPredicate),
				Query.ALL_PROPERTIES);
		final FeatureReader<SimpleFeatureType, SimpleFeature> reader = geotoolsDataStore.getFeatureReader(
				query,
				Transaction.AUTO_COMMIT);
		int count = 0;
		while (reader.hasNext()) {
			final SimpleFeature feature = reader.next();
			count++;
			Assert.assertTrue(feature.getAttribute(geometry_attribute) != null);
			Assert.assertTrue(feature.getAttribute(long_attribute) != null);
			Assert.assertTrue(feature.getAttribute(string_attribute) != null);
		}
		Assert.assertTrue(count == 3);
	}

	@Test
	public void testSubsetAttributes()
			throws CQLException,
			IOException {
		final Query query = new Query(
				typeName,
				CQL.toFilter(cqlPredicate),
				new String[] {
					geometry_attribute,
					string_attribute
				});
		final FeatureReader<SimpleFeatureType, SimpleFeature> reader = geotoolsDataStore.getFeatureReader(
				query,
				Transaction.AUTO_COMMIT);
		int count = 0;
		while (reader.hasNext()) {
			final SimpleFeature feature = reader.next();
			count++;
			Assert.assertTrue(feature.getAttribute(geometry_attribute) != null);
			Assert.assertTrue(feature.getAttribute(long_attribute) == null);
			Assert.assertTrue(feature.getAttribute(string_attribute) != null);
		}
		Assert.assertTrue(count == 3);
	}

}
