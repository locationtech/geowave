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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;

public class FeatureGeometryUtilsTest
{

	GeometryFactory factory = new GeometryFactory();

	@Test
	public void testSplit() {
		final Geometry multiPolygon = factory.createMultiPolygon(new Polygon[] {
			factory.createPolygon(new Coordinate[] {
				new Coordinate(
						179.0,
						-89),
				new Coordinate(
						179.0,
						-92),
				new Coordinate(
						182.0,
						-92),
				new Coordinate(
						192.0,
						-89),
				new Coordinate(
						179.0,
						-89)
			})
		});
		final Geometry result = FeatureGeometryUtils.adjustGeo(
				GeometryUtils.DEFAULT_CRS,
				multiPolygon);

		assertTrue(result.intersects(multiPolygon));
		assertTrue(result.getNumGeometries() == 2);
	}

	@Test
	public void testSimple() {

		final Geometry singlePoly = factory.createMultiPolygon(new Polygon[] {
			factory.createPolygon(new Coordinate[] {
				new Coordinate(
						169.0,
						20),
				new Coordinate(
						169.0,
						21),
				new Coordinate(
						172.0,
						21),
				new Coordinate(
						172.0,
						20),
				new Coordinate(
						169.0,
						20)
			})
		});
		final Geometry result = FeatureGeometryUtils.adjustGeo(
				GeometryUtils.DEFAULT_CRS,
				singlePoly);

		assertTrue(result.intersects(singlePoly));
		assertTrue(singlePoly.isValid());
		assertTrue(singlePoly.getNumGeometries() == 1);

	}
}
