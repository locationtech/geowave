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
package mil.nga.giat.geowave.adapter.vector.plugin.visibility;

import static org.junit.Assert.*;

import java.text.ParseException;

import mil.nga.giat.geowave.adapter.vector.AvroFeatureDataAdapter;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;

import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class VisibilityConfigurationTest
{
	private SimpleFeatureType schema;

	@Before
	public void setup()
			throws SchemaException,
			CQLException,
			ParseException {
		schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,pid:String");// typeBuilder.buildFeatureType();

	}

	public static class TestExtension extends
			JsonDefinitionColumnVisibilityManagement
	{

	}

	@Test
	public void test() {
		schema.getDescriptor(
				"pid").getUserData().clear();
		schema.getDescriptor(
				"pid").getUserData().put(
				"visibility",
				Boolean.TRUE);
		schema.getUserData().put(
				"visibilityManagerClass",
				TestExtension.class.getName());

		final VisibilityConfiguration config = new VisibilityConfiguration(
				schema);

		assertEquals(
				"pid",
				config.getAttributeName());
		assertEquals(
				TestExtension.class,
				config.getManager().getClass());

		config.setAttributeName("pop");
		config.setManagerClassName(JsonDefinitionColumnVisibilityManagement.class.getName());
		config.updateType(schema);

		assertEquals(
				Boolean.TRUE,
				schema.getDescriptor(
						"pop").getUserData().get(
						"visibility"));

		assertFalse(schema.getDescriptor(
				"pid").getUserData().containsKey(
				"visibility"));
		assertEquals(
				JsonDefinitionColumnVisibilityManagement.class.getName(),
				schema.getUserData().get(
						"visibilityManagerClass"));

	}
}
