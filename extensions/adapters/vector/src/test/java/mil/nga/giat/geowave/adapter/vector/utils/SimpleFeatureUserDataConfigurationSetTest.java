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
package mil.nga.giat.geowave.adapter.vector.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;

import org.geotools.feature.SchemaException;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeatureType;

public class SimpleFeatureUserDataConfigurationSetTest
{

	@Test
	public void testNoConfig()
			throws SchemaException {
		SimpleFeatureType type = FeatureDataUtils.decodeType(
				"http://somens.org",
				"type1",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,pid:String,vis:String",
				"east");
		SimpleFeatureUserDataConfigurationSet.configureType(type);
		assertFalse(type.getDescriptor(
				"pop").getUserData().containsKey(
				"stats"));
	}

	@Test
	public void testConfig()
			throws SchemaException {
		System.setProperty(
				SimpleFeatureUserDataConfigurationSet.SIMPLE_FEATURE_CONFIG_FILE_PROP,
				"src/test/resources/statsFile.json");
		SimpleFeatureType type = FeatureDataUtils.decodeType(
				"http://somens.org",
				"type1",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,pid:String,vis:String",
				"east");
		SimpleFeatureUserDataConfigurationSet.configureType(type);
		assertTrue(type.getDescriptor(
				"pop").getUserData().containsKey(
				"stats"));
		assertTrue(type.getDescriptor(
				"vis").getUserData().containsKey(
				"visibility"));
		assertTrue(type.getUserData().containsKey(
				"visibilityManagerClass"));
		assertTrue(type.getUserData().get(
				"PrimaryIndexName").equals(
				"SPATIAL_IDX"));
	}
}
