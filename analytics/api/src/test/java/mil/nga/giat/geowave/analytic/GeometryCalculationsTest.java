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
package mil.nga.giat.geowave.analytic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import javax.measure.unit.SI;

import mil.nga.giat.geowave.analytic.GeometryCalculations;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.junit.Test;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

public class GeometryCalculationsTest
{

	@Test
	public void test()
			throws NoSuchAuthorityCodeException,
			FactoryException,
			TransformException {
		CoordinateReferenceSystem crs = CRS.decode(
				"EPSG:4326",
				true);

		GeometryCalculations calculator = new GeometryCalculations(
				crs);
		List<Geometry> geos = calculator.buildSurroundingGeometries(
				new double[] {
					50,
					50
				},
				SI.KILOMETER,
				new Coordinate(
						30,
						30));
		assertEquals(
				1,
				geos.size());
		Geometry geo = geos.get(0);
		double lastDist = Double.NaN;
		Coordinate lastCoord = null;
		for (Coordinate coord : geo.getCoordinates()) {
			if (lastCoord != null) {
				final double dist = JTS.orthodromicDistance(
						lastCoord,
						coord,
						crs);
				// scaling on the globe...so not perfect square
				assertEquals(
						Math.abs(dist),
						100000,
						500);
			}
			final double dist = JTS.orthodromicDistance(
					geo.getCentroid().getCoordinate(),
					coord,
					crs);
			// distances are roughly even to all corners
			if (!Double.isNaN(lastDist)) {
				assertTrue(Math.abs(dist - lastDist) < 200);
			}
			lastDist = dist;
			lastCoord = coord;
		}
		Envelope envelope = geo.getEnvelopeInternal();
		assertTrue(envelope.getMaxX() > 30);
		assertTrue(envelope.getMinX() < 30);
		assertTrue(envelope.getMaxY() > 30);
		assertTrue(envelope.getMinX() < 30);

		geos = calculator.buildSurroundingGeometries(
				new double[] {
					100,
					100
				},
				SI.KILOMETER,
				new Coordinate(
						179.9999999996,
						0));
		assertEquals(
				2,
				geos.size());
		geo = geos.get(0);
		envelope = geo.getEnvelopeInternal();
		assertTrue(envelope.getMaxX() < -179 && envelope.getMaxX() > -180);
		assertEquals(
				-180.0,
				envelope.getMinX(),
				0.0000001);

		geo = geos.get(1);
		envelope = geo.getEnvelopeInternal();
		assertTrue(envelope.getMinX() < 180 && envelope.getMinX() > 179);
		assertEquals(
				180.0,
				envelope.getMaxX(),
				0.0000001);

	}
}
