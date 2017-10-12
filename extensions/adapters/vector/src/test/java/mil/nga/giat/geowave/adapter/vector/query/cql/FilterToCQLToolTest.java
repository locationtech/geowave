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
package mil.nga.giat.geowave.adapter.vector.query.cql;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;

import org.apache.commons.lang3.tuple.Pair;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.filter.FilterFactoryImpl;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.Id;
import org.opengis.filter.expression.Expression;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class FilterToCQLToolTest
{

	SimpleFeatureType type;

	@Before
	public void setup()
			throws SchemaException,
			CQLException {
		type = DataUtilities.createType(
				"geostuff",
				"geom:Geometry:srid=4326,pop:java.lang.Long,pid:String");

	}

	@Test
	public void testDate()
			throws CQLException {
		assertNotNull(FilterToCQLTool.toFilter("when = 2005-05-19T21:32:56Z"));
	}

	@Test
	public void tesFid() {
		final FilterFactoryImpl factory = new FilterFactoryImpl();
		final Id f = factory.id(new FeatureIdImpl(
				"123-abc"));
		final String ss = ECQL.toCQL(f);
		System.out.println(ss);
		assertTrue(ss.contains("'123-abc'"));

	}

	@Test
	public void test() {
		final FilterFactoryImpl factory = new FilterFactoryImpl();
		final Expression exp1 = factory.property("pid");
		final Expression exp2 = factory.literal("a89dhd-123-abc");
		final Filter f = factory.equal(
				exp1,
				exp2,
				false);
		final String ss = ECQL.toCQL(f);
		assertTrue(ss.contains("'a89dhd-123-abc'"));
	}

	@Test
	public void testDWithinFromCQLFilter()
			throws CQLException {
		final Filter filter = CQL.toFilter("DWITHIN(geom, POINT(-122.7668 0.4979), 233.7, meters)");
		final String gtFilterStr = ECQL.toCQL(FilterToCQLTool.fixDWithin(filter));
		System.out.println(gtFilterStr);
		assertTrue(gtFilterStr.contains("INTERSECTS(geom, POLYGON (("));

		testFilter(FilterToCQLTool.toFilter(gtFilterStr));
	}

	@Test
	public void testDWithinFromTool()
			throws CQLException {
		testFilter(FilterToCQLTool.toFilter("DWITHIN(geom, POINT(-122.7668 0.4979), 233.7, meters)"));
	}

	public void testFilter(
			Filter gtFilter ) {

		final SimpleFeature newFeature = FeatureDataUtils.buildFeature(
				type,
				new Pair[] {
					Pair.of(
							"geom",
							new GeometryFactory().createPoint(new Coordinate(
									-122.76570055844142,
									0.4979))),
					Pair.of(
							"pop",
							Long.valueOf(100))

				});

		assertTrue(gtFilter.evaluate(newFeature));

		final SimpleFeature newFeatureToFail = FeatureDataUtils.buildFeature(
				type,
				new Pair[] {
					Pair.of(
							"geom",
							new GeometryFactory().createPoint(new Coordinate(
									-122.7690,
									0.4980))),
					Pair.of(
							"pop",
							Long.valueOf(100))

				});

		assertFalse(gtFilter.evaluate(newFeatureToFail));

	}

}
