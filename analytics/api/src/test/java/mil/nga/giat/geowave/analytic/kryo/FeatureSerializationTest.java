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
package mil.nga.giat.geowave.analytic.kryo;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.UUID;

import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.InputChunked;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.OutputChunked;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class FeatureSerializationTest
{

	@Test
	public void test()
			throws SchemaException {
		final Kryo kryo = new Kryo();

		kryo.register(
				SimpleFeatureImpl.class,
				new FeatureSerializer());

		final SimpleFeatureType schema = DataUtilities.createType(
				"testGeo",
				"location:Point:srid=4326,name:String");
		final List<AttributeDescriptor> descriptors = schema.getAttributeDescriptors();
		final Object[] defaults = new Object[descriptors.size()];
		int p = 0;
		for (final AttributeDescriptor descriptor : descriptors) {
			defaults[p++] = descriptor.getDefaultValue();
		}

		final SimpleFeature feature = SimpleFeatureBuilder.build(
				schema,
				defaults,
				UUID.randomUUID().toString());
		final GeometryFactory geoFactory = new GeometryFactory();

		feature.setAttribute(
				"location",
				geoFactory.createPoint(new Coordinate(
						-45,
						45)));
		final Output output = new OutputChunked();
		kryo.getSerializer(
				SimpleFeatureImpl.class).write(
				kryo,
				output,
				feature);
		final Input input = new InputChunked();
		input.setBuffer(output.getBuffer());
		final SimpleFeature f2 = (SimpleFeature) kryo.getSerializer(
				SimpleFeatureImpl.class).read(
				kryo,
				input,
				SimpleFeatureImpl.class);
		assertEquals(
				feature,
				f2);

	}
}
