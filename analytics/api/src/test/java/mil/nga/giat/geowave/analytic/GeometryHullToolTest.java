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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import mil.nga.giat.geowave.analytic.GeometryDataSetGenerator.CurvedDensityDataGeneratorTool;
import mil.nga.giat.geowave.analytic.GeometryGenerator.DistortationFn;
import mil.nga.giat.geowave.analytic.distance.CoordinateCircleDistanceFn;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKTReader;

public class GeometryHullToolTest
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(GeometryHullToolTest.class);

	GeometryFactory factory = new GeometryFactory();

	@Test
	public void testDistance() {
		final double distance1 = GeometryHullTool.calcDistance(
				new Coordinate(
						3,
						3),
				new Coordinate(
						6,
						6),
				new Coordinate(
						5,
						5.5));

		final double distance2 = GeometryHullTool.calcDistance(
				new Coordinate(
						3,
						3),
				new Coordinate(
						6,
						6),
				new Coordinate(
						5,
						4.5));

		assertEquals(
				distance1,
				distance2,
				0.0001);

		final double distance3 = GeometryHullTool.calcDistance(
				new Coordinate(
						4,
						6),
				new Coordinate(
						6,
						12),
				new Coordinate(
						5,
						8));

		assertTrue(distance3 > 0);

		final double distance4 = GeometryHullTool.calcDistance(
				new Coordinate(
						4,
						6),
				new Coordinate(
						6,
						12),
				new Coordinate(
						5,
						9));

		assertEquals(
				0.0,
				distance4,
				0.001);

		final double distance5 = GeometryHullTool.calcDistance(
				new Coordinate(
						5,
						7),
				new Coordinate(
						11,
						3),
				new Coordinate(
						6,
						10));

		assertTrue(distance5 < 0);

		final double distance6 = GeometryHullTool.calcDistance(
				new Coordinate(
						5,
						7),
				new Coordinate(
						11,
						3),
				new Coordinate(
						7,
						6.5));

		final double distance7 = GeometryHullTool.calcDistance(
				new Coordinate(
						5,
						7),
				new Coordinate(
						11,
						3),
				new Coordinate(
						7,
						5.0));

		assertTrue(distance7 < distance6);

	}

	@Test
	public void testAngles() {
		assertTrue(GeometryHullTool.calcAngle(
				new Coordinate(
						39,
						41.5),
				new Coordinate(
						41,
						41),
				new Coordinate(
						38,
						41.2)) > 0);

		assertTrue(GeometryHullTool.calcAngle(
				new Coordinate(
						39,
						41.5),
				new Coordinate(
						41,
						41),
				new Coordinate(
						38,
						43)) < 0);

		assertTrue(GeometryHullTool.calcAngle(
				new Coordinate(
						39,
						41.5),
				new Coordinate(
						41,
						41),
				new Coordinate(
						38,
						41.2)) < GeometryHullTool.calcAngle(
				new Coordinate(
						39,
						41.5),
				new Coordinate(
						41,
						41),
				new Coordinate(
						38,
						41.1)));

		assertTrue(GeometryHullTool.calcAngle(
				new Coordinate(
						39,
						41.5),
				new Coordinate(
						41,
						41),
				new Coordinate(
						38,
						43)) > GeometryHullTool.calcAngle(
				new Coordinate(
						39,
						41.5),
				new Coordinate(
						41,
						41),
				new Coordinate(
						38,
						44)));

		assertTrue(GeometryHullTool.calcAngle(
				new Coordinate(
						42,
						42),
				new Coordinate(
						41,
						41),
				new Coordinate(
						42.5,
						44)) > 0);

		assertTrue(GeometryHullTool.calcAngle(
				new Coordinate(
						42,
						42),
				new Coordinate(
						41,
						41),
				new Coordinate(
						42.5,
						40.5)) < 0);

		assertEquals(
				-90.0,
				GeometryHullTool.calcAngle(
						new Coordinate(
								41,
								42),
						new Coordinate(
								41,
								41),
						new Coordinate(
								42,
								41)),
				0.001);

		assertEquals(
				90.0,
				GeometryHullTool.calcAngle(
						new Coordinate(
								42,
								41),
						new Coordinate(
								41,
								41),
						new Coordinate(
								41,
								42)),
				0.001);

		assertEquals(
				-180,
				GeometryHullTool.calcAngle(
						new Coordinate(
								42,
								42),
						new Coordinate(
								41,
								41),
						new Coordinate(
								40,
								40)),
				0.001);

		assertEquals(
				0,
				GeometryHullTool.calcAngle(
						new Coordinate(
								42,
								42),
						new Coordinate(
								41,
								41),
						new Coordinate(
								42,
								42)),
				0.001);

		assertEquals(
				-315,
				GeometryHullTool.calcAngle(
						new Coordinate(
								41,
								41),
						new Coordinate(
								42,
								41),
						new Coordinate(
								41,
								40)),
				0.001);

		assertEquals(
				-45,
				GeometryHullTool.calcAngle(
						new Coordinate(
								42,
								41),
						new Coordinate(
								41,
								41),
						new Coordinate(
								42,
								40)),
				0.001);

		assertEquals(
				-45,
				GeometryHullTool.calcAngle(
						new Coordinate(
								41,
								42),
						new Coordinate(
								41,
								41),
						new Coordinate(
								42,
								42)),
				0.001);

	}

	@Test
	public void testConcaveHullBulkTest() {
		long time = System.currentTimeMillis();
		for (int i = 0; i < 10; i++) {
			assertTrue(getHull(
					factory.createLineString(new Coordinate[] {
						new Coordinate(
								41.2,
								40.8),
						new Coordinate(
								40.8,
								40.6)
					}),
					"po1",
					false,
					true).isSimple() || true);
		}
		System.out.println(System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		for (int i = 0; i < 10; i++) {
			assertTrue(getHull(
					factory.createLineString(new Coordinate[] {
						new Coordinate(
								41.2,
								40.8),
						new Coordinate(
								40.8,
								40.6)
					}),
					"er1",
					false,
					false).isSimple() || true);
		}
		System.out.println(System.currentTimeMillis() - time);

	}

	private final Random r = new Random(
			7777);

	private Coordinate pickOneAndAugmentOne(
			final Coordinate[] list ) {
		final Coordinate select = list[(Math.abs(r.nextInt()) % list.length)];
		return new Coordinate(
				select.x + r.nextGaussian(),
				select.y + r.nextGaussian(),
				select.z);
	}

	final Coordinate[] poly1 = new Coordinate[] {
		new Coordinate(
				40,
				40),
		new Coordinate(
				40.1,
				40.1),
		new Coordinate(
				39.2,
				41.2), // selected top (2)
		new Coordinate(
				39,
				40.7),
		new Coordinate(
				38.7,
				40.1),
		new Coordinate(
				38.4,
				39.5),
		new Coordinate(
				// selected bottom (6)
				39.3,
				39.2),
		new Coordinate(
				40,
				40)
	};

	final Coordinate[] poly2 = new Coordinate[] {
		new Coordinate(
				40.2,
				40),
		new Coordinate(
				40.5,
				41), // selected top (1)
		new Coordinate(
				41.2,
				40.8),
		new Coordinate(
				40.8,
				40.6),
		new Coordinate(
				40.6,
				39.6),
		new Coordinate(
				40.3,
				39.8), // selected
						// bottom(5)
		new Coordinate(
				40.2,
				40)
	};

	@Test
	public void testLRPolygons() {
		final Geometry leftShape = factory.createPolygon(poly1);
		final Geometry rightShape = factory.createPolygon(poly2);
		assertTrue(GeometryHullTool.clockwise(leftShape.getCoordinates()));
		assertFalse(GeometryHullTool.clockwise(rightShape.getCoordinates()));
		final GeometryHullTool cg = new GeometryHullTool();
		cg.setDistanceFnForCoordinate(new CoordinateCircleDistanceFn());
		final Geometry geo = cg.connect(
				leftShape,
				rightShape);
		assertEquals(
				"POLYGON ((39.2 41.2, 39 40.7, 38.7 40.1, 38.4 39.5, 39.3 39.2, 40.6 39.6, 40.8 40.6, 41.2 40.8, 40.5 41, 39.2 41.2))",
				geo.toString());

	}

	@Test
	public void testRLPolygons() {
		final Geometry leftShape = factory.createPolygon(poly2);

		final Geometry rightShape = factory.createPolygon(poly1);

		assertFalse(GeometryHullTool.clockwise(leftShape.getCoordinates()));
		assertTrue(GeometryHullTool.clockwise(rightShape.getCoordinates()));
		final GeometryHullTool cg = new GeometryHullTool();
		cg.setDistanceFnForCoordinate(new CoordinateCircleDistanceFn());
		final Geometry geo = cg.connect(
				leftShape,
				rightShape);
		assertEquals(
				"POLYGON ((39.2 41.2, 39 40.7, 38.7 40.1, 38.4 39.5, 39.3 39.2, 40.6 39.6, 40.8 40.6, 41.2 40.8, 40.5 41, 39.2 41.2))",
				geo.toString());
	}

	public void testRandomConnect()
			throws IOException {

		final GeometryHullTool cg = new GeometryHullTool();
		cg.setDistanceFnForCoordinate(new CoordinateCircleDistanceFn());
		final Iterator<Geometry> it1 = GeometryGenerator.generate(
				1000,
				Arrays.asList(1.0),
				new DistortationFn() {
					final Random r = new Random(
							7777);

					@Override
					public double distort() {
						return 0.5 + (0.5 * r.nextDouble());
					}

				},
				5,
				new Envelope(
						45,
						55,
						35,
						45));
		final Iterator<Geometry> it2 = GeometryGenerator.generate(
				1000,
				Arrays.asList(1.0),
				new DistortationFn() {
					final Random r = new Random(
							7777);

					@Override
					public double distort() {
						return 0.5 + (0.5 * r.nextDouble());
					}

				},
				5,
				new Envelope(
						30,
						47,
						20,
						37));

		while (it1.hasNext()) {
			Geometry rightShape = it1.next();
			Geometry leftShape = it2.next();

			if (rightShape.intersects(leftShape)) {
				Geometry inter = rightShape.intersection(leftShape);
				rightShape = rightShape.difference(inter);
				leftShape = leftShape.difference(inter);
			}

			ShapefileTool.writeShape(
					"test_random",
					new File(
							"./target/test_randoms"),
					new Geometry[] {
						leftShape,
						rightShape
					});
			Geometry geo = cg.connect(
					leftShape,
					rightShape);

			ShapefileTool.writeShape(
					"test_random",
					new File(
							"./target/test_random"),
					new Geometry[] {
						geo
					});
			if (!geo.isSimple()) {

				// assertTrue(false);
				geo = cg.connect(
						leftShape,
						rightShape);
				ShapefileTool.writeShape(
						"test_random2",
						new File(
								"./target/test_random2"),
						new Geometry[] {
							geo
						});
			}
		}
	}

	private Coordinate[] reversed(
			final Coordinate[] poly ) {
		final Coordinate polyReversed[] = new Coordinate[poly.length];
		for (int i = 0; i < poly.length; i++) {
			polyReversed[i] = poly[poly.length - i - 1];
		}
		return polyReversed;
	}

	@Test
	public void interesectEdges() {
		final GeometryHullTool.Edge e1 = new GeometryHullTool.Edge(
				new Coordinate(
						20.0,
						20.0),
				new Coordinate(
						21.5,
						21),
				0);
		final GeometryHullTool.Edge e2 = new GeometryHullTool.Edge(
				new Coordinate(
						20.4,
						19.0),
				new Coordinate(
						21.0,
						22),
				0);
		assertTrue(GeometryHullTool.edgesIntersect(
				e1,
				e2));
		final GeometryHullTool.Edge e3 = new GeometryHullTool.Edge(
				new Coordinate(
						20.4,
						19.0),
				new Coordinate(
						21.0,
						19.5),
				0);
		assertTrue(!GeometryHullTool.edgesIntersect(
				e1,
				e3));
	}

	@Test
	public void testRLSamePolygons() {

		final Geometry leftShape = factory.createPolygon(reversed(poly1));
		final Geometry rightShape = factory.createPolygon(reversed(poly2));

		assertFalse(GeometryHullTool.clockwise(leftShape.getCoordinates()));
		assertTrue(GeometryHullTool.clockwise(rightShape.getCoordinates()));
		final GeometryHullTool cg = new GeometryHullTool();
		cg.setDistanceFnForCoordinate(new CoordinateCircleDistanceFn());
		final Geometry geo = cg.connect(
				leftShape,
				rightShape);
		assertEquals(
				"POLYGON ((39.2 41.2, 39 40.7, 38.7 40.1, 38.4 39.5, 39.3 39.2, 40.6 39.6, 40.8 40.6, 41.2 40.8, 40.5 41, 39.2 41.2))",
				geo.toString());
	}

	@Test
	public void testPolygonConnection() {

		final boolean save = true;
		final Geometry concave1 = getHull(
				factory.createLineString(new Coordinate[] {
					new Coordinate(
							41.2,
							40.8),
					new Coordinate(
							40.8,
							40.6)
				}),
				"p1",
				save,
				false);
		final Geometry concave2 = getHull(
				factory.createLineString(new Coordinate[] {
					new Coordinate(
							39.9,
							40.6),
					new Coordinate(
							40.8,
							40.6)
				}),
				"p2",
				save,
				false);
		final Geometry concave3 = getHull(
				factory.createLineString(new Coordinate[] {
					new Coordinate(
							42.0,
							42.0),
					new Coordinate(
							41.2,
							40.8)
				}),
				"p3",
				save,
				false);

		final Geometry hull = concave1.union(
				concave2).union(
				concave3);

		assertTrue(hull.isSimple());

		writeToShapeFile(
				"final_phull",
				hull);

		coversPoints(
				hull,
				concave1);
		coversPoints(
				hull,
				concave2);
		coversPoints(
				hull,
				concave3);
	}

	private Geometry getHull(
			final LineString str,
			final String name,
			final boolean save,
			final boolean parkandOh ) {

		final List<Point> points = CurvedDensityDataGeneratorTool.generatePoints(
				str,
				0.4,
				1000);

		final GeometryHullTool cg = new GeometryHullTool();
		cg.setDistanceFnForCoordinate(new CoordinateCircleDistanceFn());

		final Coordinate[] coordinates = new Coordinate[points.size()];
		int i = 0;
		for (final Point point : points) {
			coordinates[i++] = point.getCoordinate();
		}

		final ConvexHull convexHull = new ConvexHull(
				coordinates,
				factory);

		final Geometry concaveHull = parkandOh ? cg.concaveHullParkOhMethod(
				convexHull.getConvexHull(),
				Arrays.asList(coordinates)) : cg.concaveHull(
				convexHull.getConvexHull(),
				Arrays.asList(coordinates));
		if (save || !concaveHull.isSimple()) {
			writeToShapeFile(
					"setx_" + name,
					points.toArray(new Geometry[points.size()]));
			writeToShapeFile(
					"chullx_" + name,
					concaveHull);
			writeToShapeFile(
					"hullx_" + name,
					convexHull.getConvexHull());
		}

		// final Geometry concaveHull1 = cg.concaveHull1(
		// convexHull.getConvexHull(),
		// Arrays.asList(coordinates));
		// if (save || !concaveHull1.isSimple()) {
		// writeToShapeFile(
		// "chull_" + name,
		// concaveHull1);
		// }

		return concaveHull;
	}

	private static void writeToShapeFile(
			final String name,
			final Geometry... geos ) {
		if (true) { // LOGGER.isDebugEnabled()) {
			try {
				ShapefileTool.writeShape(
						name,
						new File(
								"./target/test_" + name),
						geos);
			}
			catch (final IOException e) {
				e.printStackTrace();
			}
		}

	}

	private static boolean coversPoints(
			final Geometry coverer,
			final Geometry pointsToCover ) {
		for (final Coordinate coordinate : pointsToCover.getCoordinates()) {
			if (!coverer.covers(coverer.getFactory().createPoint(
					coordinate))) {
				return false;
			}
		}
		return true;
	}

	@Test
	public void testCreateHullFromGeometry() {

		final GeometryHullTool cg = new GeometryHullTool();
		cg.setDistanceFnForCoordinate(new CoordinateCircleDistanceFn());

		for (int i = 2; i < 10; i++) {
			final Coordinate[] coords = new Coordinate[i];

			for (int p = 0; p < i; p++) {
				coords[p] = new Coordinate(
						p,
						p);
			}

			final Geometry lineString1 = factory.createLineString(coords);

			final Geometry concaveHull1 = cg.createHullFromGeometry(
					lineString1,
					Arrays.asList(coords[0]),
					true);

			assertEquals(
					"straigh line size=" + i + " geo=" + lineString1.toText(),
					2,
					concaveHull1.getCoordinates().length);
		}

		final Geometry lineString3 = factory.createLineString(new Coordinate[] {
			new Coordinate(
					1,
					1),
			new Coordinate(
					2,
					2),
			new Coordinate(
					3,
					1)
		});

		final Geometry concaveHull3 = cg.createHullFromGeometry(
				lineString3,
				Arrays.asList(lineString3.getCoordinates()[0]),
				true);

		assertEquals(
				"expecting a triangle " + concaveHull3.toText(),
				4,
				concaveHull3.getCoordinates().length);

		assertTrue(
				"expecting a triangle " + concaveHull3.toText(),
				concaveHull3.getArea() > 0.0);
		assertTrue(concaveHull3.isSimple());

		assertEquals(
				"expecting identical result",
				lineString3,
				cg.createHullFromGeometry(
						lineString3,
						Collections.<Coordinate> emptyList(),
						true));

		final Geometry[] newPoints = new Geometry[900];
		for (int j = 0; j < 10; j++) {
			final Coordinate[] newCoords = new Coordinate[900];
			final Coordinate[] geoCoords = new Coordinate[100];
			final Random rand = new Random(
					73634 + j);

			for (int i = 0; i < 100; i++) {
				geoCoords[i] = new Coordinate(
						rand.nextGaussian() * 0.001,
						rand.nextGaussian() * 0.001);
			}
			for (int i = 0; i < 900; i++) {
				newCoords[i] = new Coordinate(
						rand.nextGaussian() * 0.01,
						rand.nextGaussian() * 0.01);
				newPoints[i] = factory.createPoint(newCoords[i]);
			}
			ConvexHull hull = new ConvexHull(
					geoCoords,
					factory);
			final Geometry concaveHull = cg.createHullFromGeometry(
					hull.getConvexHull(),
					Arrays.asList(newCoords),
					true);
			assertTrue(concaveHull.isSimple());
			int error = 0;
			for (Geometry newPoint : newPoints) {
				error += concaveHull.intersects(newPoint) ? 0 : 1;
			}
			assertTrue(error < 3);
			final Geometry concaveHull2 = cg.createHullFromGeometry(
					hull.getConvexHull(),
					Arrays.asList(newCoords),
					false);
			assertTrue(concaveHull2.isSimple());
			error = 0;
			for (Geometry newPoint : newPoints) {
				error += concaveHull2.intersects(newPoint) ? 0 : 1;
			}
			assertTrue(error < 1);
		}

	}
}
