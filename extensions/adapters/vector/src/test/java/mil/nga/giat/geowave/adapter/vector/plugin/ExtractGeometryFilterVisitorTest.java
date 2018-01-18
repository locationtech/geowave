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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;

import org.geotools.data.Query;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.JTS;
import org.junit.Test;
import org.opengis.filter.Filter;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter.CompareOperation;

public class ExtractGeometryFilterVisitorTest
{
	final String geomAttributeName = "geom";
	final ExtractGeometryFilterVisitor visitorWithDescriptor = new ExtractGeometryFilterVisitor(
			GeometryUtils.DEFAULT_CRS,
			geomAttributeName);

	@Test
	public void testDWithin()
			throws CQLException,
			TransformException,
			ParseException {

		final Filter filter = CQL.toFilter(String.format(
				"DWITHIN(%s, POINT(-122.7668 0.4979), 233.7, meters)",
				geomAttributeName));
		final Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);
		final Geometry geometry = result.getGeometry();
		assertNotNull(geometry);
		for (final Coordinate coord : geometry.getCoordinates()) {

			assertEquals(
					233.7,
					JTS.orthodromicDistance(
							coord,
							new Coordinate(
									-122.7668,
									0.4979),
							GeometryUtils.DEFAULT_CRS),
					2);
		}
	}

	@Test
	public void testDWithinDateLine()
			throws CQLException,
			TransformException,
			ParseException {

		final Filter filter = CQL.toFilter(String.format(
				"DWITHIN(%s, POINT(179.9998 0.79), 13.7, kilometers)",
				geomAttributeName));
		final Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);
		final Geometry geometry = result.getGeometry();
		assertNotNull(geometry);
		for (final Coordinate coord : geometry.getCoordinates()) {

			assertEquals(
					13707.1,
					JTS.orthodromicDistance(
							coord,
							new Coordinate(
									179.9999,
									0.79),
							GeometryUtils.DEFAULT_CRS),
					2000);
		}
	}

	@Test
	public void testBBOX()
			throws CQLException,
			TransformException,
			ParseException {

		final Filter filter = CQL.toFilter(String.format(
				"BBOX(%s, 0, 0, 10, 25)",
				geomAttributeName));
		final Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.INTERSECTS);
	}

	@Test
	public void testIntersects()
			throws CQLException,
			TransformException,
			ParseException {

		final Filter filter = CQL.toFilter(String.format(
				"INTERSECTS(%s, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))",
				geomAttributeName));
		final Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.INTERSECTS);
	}

	@Test
	public void testOverlaps()
			throws CQLException,
			TransformException,
			ParseException {

		final Filter filter = CQL.toFilter(String.format(
				"OVERLAPS(%s, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))",
				geomAttributeName));
		final Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.OVERLAPS);
	}

	@Test
	public void testEquals()
			throws CQLException,
			TransformException,
			ParseException {

		final Filter filter = CQL.toFilter(String.format(
				"EQUALS(geom, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))",
				geomAttributeName));
		final Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.EQUALS);
	}

	@Test
	public void testCrosses()
			throws CQLException,
			TransformException,
			ParseException {

		final Filter filter = CQL.toFilter(String.format(
				"CROSSES(%s, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))",
				geomAttributeName));
		final Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.CROSSES);
	}

	@Test
	public void testTouches()
			throws CQLException,
			TransformException,
			ParseException {

		final Filter filter = CQL.toFilter(String.format(
				"TOUCHES(%s, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))",
				geomAttributeName));
		final Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.TOUCHES);
	}

	@Test
	public void testWithin()
			throws CQLException,
			TransformException,
			ParseException {

		final Filter filter = CQL.toFilter(String.format(
				"WITHIN(%s, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))",
				geomAttributeName));
		final Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.CONTAINS);
	}

	@Test
	public void testContains()
			throws CQLException,
			TransformException,
			ParseException {

		final Filter filter = CQL.toFilter(String.format(
				"CONTAINS(geom, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))",
				geomAttributeName));
		final Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.WITHIN);
	}

	@Test
	public void testDisjoint()
			throws CQLException,
			TransformException,
			ParseException {

		final Filter filter = CQL.toFilter(String.format(
				"DISJOINT(%s, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))",
				geomAttributeName));
		final Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);
		// for non-inclusive filters we can't extract query geometry and
		// predicate
		// assertTrue(Double.isNaN(result.getGeometry().getArea()));
		assertTrue(result.getCompareOp() == null);
	}

	@Test
	public void testIntesectAndBBox()
			throws CQLException,
			TransformException,
			ParseException {

		// BBOX geometry is completely contained within Intersects geometry
		// we are testing to see if we are able to combine simple geometric
		// relations with similar predicates
		// into a single query geometry/predicate
		final Filter filter = CQL.toFilter(String.format(
				"INTERSECTS(%s, POLYGON((0 0, 0 50, 20 50, 20 0, 0 0))) AND BBOX(%s, 0, 0, 10, 25)",
				geomAttributeName,
				geomAttributeName));
		final Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == CompareOperation.INTERSECTS);
	}

	@Test
	public void testIntesectAndCrosses()
			throws CQLException,
			TransformException,
			ParseException {

		// CROSSES geometry is completely contained within INTERSECT geometry
		// we are testing to see if we are able to combine dissimilar geometric
		// relations correctly
		// to extract query geometry. Note, we can't combine two different
		// predicates into one but
		// we can combine geometries for the purpose of deriving linear
		// constraints
		final Filter filter = CQL
				.toFilter(String
						.format(
								"INTERSECTS(%s, POLYGON((0 0, 0 50, 20 50, 20 0, 0 0))) AND CROSSES(%s, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))",
								geomAttributeName,
								geomAttributeName));
		final Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == null);
	}

	@Test
	public void testOverlapsOrCrosses()
			throws CQLException,
			TransformException,
			ParseException {

		// TOUCHES geometry is completely contained within OVERLAPS geometry
		// we are testing to see if we are able to combine dissimilar geometric
		// relations correctly
		// to extract query geometry. Note, we can't combine two different
		// predicates into one but
		// we can combine geometries for the purpose of deriving linear
		// constraints
		final Filter filter = CQL
				.toFilter(String
						.format(
								"OVERLAPS(%s, POLYGON((0 0, 0 50, 20 50, 20 0, 0 0))) OR TOUCHES(%s, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0)))",
								geomAttributeName,
								geomAttributeName));
		final Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				20,
				0,
				50);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == null);
	}

	@Test
	public void testIntesectAndCrossesAndLike()
			throws CQLException,
			TransformException,
			ParseException {

		// we are testing to see if we are able to combine dissimilar geometric
		// relations correctly
		// to extract query geometry. Note, that returned predicate is null
		// since we can't represent
		// CQL expression fully into single query geometry and predicate
		final Filter filter = CQL.toFilter(String.format(
				"CROSSES(%s, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0))) AND location == 'abc'",
				geomAttributeName));
		final Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == null);
	}

	@Test
	public void testWithMultipleAttributes()
			throws CQLException,
			TransformException,
			ParseException {

		// In this test query, we have constrains over multiple geometric
		// attributes.
		// The ExtractGeometryFilterVisitor class should only extracts
		// geometric constrains associated with the specified attribute name and
		// ignore others.
		final Filter filter = CQL
				.toFilter(String
						.format(
								"INTERSECTS(%s, POLYGON((0 0, 0 25, 10 25, 10 0, 0 0))) AND INTERSECTS(geomOtherAttr, POLYGON((0 0, 0 5, 5 5, 5 0, 0 0)))",
								geomAttributeName));
		final Query query = new Query(
				"type",
				filter);

		final ExtractGeometryFilterVisitorResult result = (ExtractGeometryFilterVisitorResult) query
				.getFilter()
				.accept(
						visitorWithDescriptor,
						null);

		final Envelope bounds = new Envelope(
				0,
				10,
				0,
				25);
		final Geometry bbox = new GeometryFactory().toGeometry(bounds);

		assertTrue(bbox.equalsTopo(result.getGeometry()));
		assertTrue(result.getCompareOp() == null);
	}

}
