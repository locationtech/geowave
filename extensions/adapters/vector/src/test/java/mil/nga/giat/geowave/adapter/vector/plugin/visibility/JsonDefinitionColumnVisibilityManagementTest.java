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

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;

import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class JsonDefinitionColumnVisibilityManagementTest
{

	SimpleFeatureType type;
	List<AttributeDescriptor> descriptors;
	Object[] defaults;
	SimpleFeature newFeature;
	final JsonDefinitionColumnVisibilityManagement<SimpleFeature> manager = new JsonDefinitionColumnVisibilityManagement<SimpleFeature>();
	final GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));
	final FieldVisibilityHandler<SimpleFeature, Object> simplePIDHandler = manager.createVisibilityHandler(
			"pid",
			new GlobalVisibilityHandler<SimpleFeature, Object>(
					"default"),
			"vis");

	final FieldVisibilityHandler<SimpleFeature, Object> simplePOPHandler = manager.createVisibilityHandler(
			"pop",
			new GlobalVisibilityHandler<SimpleFeature, Object>(
					"default"),
			"vis");

	final FieldVisibilityHandler<SimpleFeature, Object> simpleGEOHandler = manager.createVisibilityHandler(
			"geometry",
			new GlobalVisibilityHandler<SimpleFeature, Object>(
					"default"),
			"vis");

	@Before
	public void setup()
			throws SchemaException,
			CQLException {
		type = DataUtilities.createType(
				"geostuff",
				"geometry:Geometry:srid=4326,vis:java.lang.String,pop:java.lang.Long,pid:String");
		descriptors = type.getAttributeDescriptors();
		defaults = new Object[descriptors.size()];
		int p = 0;
		for (final AttributeDescriptor descriptor : descriptors) {
			defaults[p++] = descriptor.getDefaultValue();
		}

		newFeature = SimpleFeatureBuilder.build(
				type,
				defaults,
				UUID.randomUUID().toString());
		newFeature.setAttribute(
				"pop",
				Long.valueOf(100));
		newFeature.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		newFeature.setAttribute(
				"vis",
				"{\"pid\":\"TS\", \"geo.*\":\"S\"}");
		newFeature.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						43.454,
						128.232)));
	}

	@Test
	public void testPIDNonDefault() {

		assertTrue(Arrays.equals(
				"TS".getBytes(StringUtils.GEOWAVE_CHAR_SET),
				simplePIDHandler.getVisibility(
						newFeature,
						new ByteArrayId(
								"pid".getBytes(StringUtils.GEOWAVE_CHAR_SET)),
						"pid")));
	}

	@Test
	public void testPOPNonDefault() {
		assertTrue(Arrays.equals(
				"default".getBytes(StringUtils.GEOWAVE_CHAR_SET),
				simplePOPHandler.getVisibility(
						newFeature,
						new ByteArrayId(
								"pop".getBytes(StringUtils.GEOWAVE_CHAR_SET)),
						"pop")));

	}

	@Test
	public void testGEORegexDefault() {
		assertTrue(Arrays.equals(
				"S".getBytes(StringUtils.GEOWAVE_CHAR_SET),
				simpleGEOHandler.getVisibility(
						newFeature,
						new ByteArrayId(
								"geometry".getBytes(StringUtils.GEOWAVE_CHAR_SET)),
						"geometry")));

	}

	@Test
	public void testCatchAllRegexDefault() {
		newFeature.setAttribute(
				"vis",
				"{\"pid\":\"TS\", \".*\":\"U\"}");
		assertTrue(Arrays.equals(
				"U".getBytes(StringUtils.GEOWAVE_CHAR_SET),
				simplePOPHandler.getVisibility(
						newFeature,
						new ByteArrayId(
								"pop".getBytes(StringUtils.GEOWAVE_CHAR_SET)),
						"pop")));

	}

}
