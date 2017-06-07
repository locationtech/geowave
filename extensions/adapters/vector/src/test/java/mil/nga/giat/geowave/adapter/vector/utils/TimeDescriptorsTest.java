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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors.TimeDescriptorConfiguration;

import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeatureType;

public class TimeDescriptorsTest
{

	@Test
	public void testOneTime()
			throws SchemaException {
		SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,pid:String");

		final TimeDescriptorConfiguration timeConfig = new TimeDescriptorConfiguration();
		timeConfig.configureFromType(schema);

		TimeDescriptors td = new TimeDescriptors(
				schema,
				timeConfig);
		assertEquals(
				"when",
				td.getTime().getLocalName());
		assertNull(td.getStartRange());
		assertNull(td.getEndRange());
		assertTrue(td.hasTime());

	}

	@Test
	public void testRangeTime()
			throws SchemaException {
		SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,start:Date,end:Date,pid:String");
		final TimeDescriptorConfiguration timeConfig = new TimeDescriptorConfiguration();
		timeConfig.configureFromType(schema);

		TimeDescriptors td = new TimeDescriptors(
				schema,
				timeConfig);
		assertEquals(
				"start",
				td.getStartRange().getLocalName());
		assertEquals(
				"end",
				td.getEndRange().getLocalName());
		assertNull(td.getTime());
		assertTrue(td.hasTime());

	}

	@Test
	public void testMixedTime()
			throws SchemaException {
		SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,start:Date,end:Date,pid:String");
		final TimeDescriptorConfiguration timeConfig = new TimeDescriptorConfiguration();
		timeConfig.configureFromType(schema);
		TimeDescriptors td = new TimeDescriptors(
				schema,
				timeConfig);
		assertEquals(
				"start",
				td.getStartRange().getLocalName());
		assertEquals(
				"end",
				td.getEndRange().getLocalName());
		assertNull(td.getTime());
		assertTrue(td.hasTime());

	}

	@Test
	public void testJustStartTime()
			throws SchemaException {
		SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,start:Date,pid:String");
		final TimeDescriptorConfiguration timeConfig = new TimeDescriptorConfiguration();
		timeConfig.configureFromType(schema);
		TimeDescriptors td = new TimeDescriptors(
				schema,
				timeConfig);
		assertEquals(
				"start",
				td.getTime().getLocalName());
		assertNull(td.getStartRange());
		assertNull(td.getEndRange());
		assertTrue(td.hasTime());

	}

	@Test
	public void testJustEndTime()
			throws SchemaException {
		SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,end:Date,pid:String");
		final TimeDescriptorConfiguration timeConfig = new TimeDescriptorConfiguration();
		timeConfig.configureFromType(schema);
		TimeDescriptors td = new TimeDescriptors(
				schema,
				timeConfig);
		assertEquals(
				"end",
				td.getTime().getLocalName());
		assertNull(td.getStartRange());
		assertNull(td.getEndRange());
		assertTrue(td.hasTime());

	}

	@Test
	public void testWhenAndEndTime()
			throws SchemaException {
		SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,end:Date,pid:String");
		final TimeDescriptorConfiguration timeConfig = new TimeDescriptorConfiguration();
		timeConfig.configureFromType(schema);
		TimeDescriptors td = new TimeDescriptors(
				schema,
				timeConfig);
		assertEquals(
				"when",
				td.getTime().getLocalName());
		assertNull(td.getStartRange());
		assertNull(td.getEndRange());
		assertTrue(td.hasTime());

	}

	@Test
	public void testWhenAndStartTime()
			throws SchemaException {
		SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,start:Date,pid:String");
		final TimeDescriptorConfiguration timeConfig = new TimeDescriptorConfiguration();
		timeConfig.configureFromType(schema);
		TimeDescriptors td = new TimeDescriptors(
				schema,
				timeConfig);
		assertEquals(
				"when",
				td.getTime().getLocalName());
		assertNull(td.getStartRange());
		assertNull(td.getEndRange());
		assertTrue(td.hasTime());

	}

}
