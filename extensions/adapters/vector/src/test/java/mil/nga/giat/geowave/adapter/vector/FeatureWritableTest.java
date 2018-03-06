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
package mil.nga.giat.geowave.adapter.vector;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.lang3.tuple.Pair;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;

import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;

public class FeatureWritableTest
{
	GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));

	@Test
	public void test()
			throws IOException {

		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName("test");
		typeBuilder.setCRS(GeometryUtils.DEFAULT_CRS); // <- Coordinate
		// reference
		// add attributes in order
		typeBuilder.add(
				"geom",
				Point.class);
		typeBuilder.add(
				"name",
				String.class);
		typeBuilder.add(
				"count",
				Long.class);

		// build the type
		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				typeBuilder.buildFeatureType());

		final SimpleFeatureType featureType = builder.getFeatureType();

		@SuppressWarnings("unchecked")
		final SimpleFeature newFeature = FeatureDataUtils.buildFeature(
				featureType,
				new Pair[] {
					Pair.of(
							"geom",
							factory.createPoint(new Coordinate(
									27.25,
									41.25))),
					Pair.of(
							"count",
							Long.valueOf(100))

				});

		final FeatureWritable writable = new FeatureWritable(
				featureType,
				newFeature);

		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try (DataOutputStream dos = new DataOutputStream(
				bos)) {
			writable.write(dos);
			dos.flush();
		}

		final ByteArrayInputStream bis = new ByteArrayInputStream(
				bos.toByteArray());
		try (DataInputStream is = new DataInputStream(
				bis)) {
			writable.readFields(is);
		}

		assertEquals(
				newFeature.getDefaultGeometry(),
				writable.getFeature().getDefaultGeometry());
		assertEquals(
				featureType.getCoordinateReferenceSystem().getCoordinateSystem(),
				writable.getFeature().getFeatureType().getCoordinateReferenceSystem().getCoordinateSystem());

	}

}
