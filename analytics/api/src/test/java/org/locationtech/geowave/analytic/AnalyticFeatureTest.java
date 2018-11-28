/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.analytic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.geotools.feature.type.BasicFeatureTypes;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.Test;
import org.locationtech.geowave.analytic.AnalyticFeature;
import org.locationtech.geowave.analytic.AnalyticFeature.ClusterFeatureAttribute;
import org.locationtech.geowave.analytic.clustering.ClusteringUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;

public class AnalyticFeatureTest
{
	@Test
	public void testGeometryCreation()
			throws MismatchedDimensionException,
			NoSuchAuthorityCodeException,
			FactoryException,
			CQLException,
			ParseException {
		final SimpleFeatureType ftype = AnalyticFeature.createGeometryFeatureAdapter(
				"centroid",
				new String[] {
					"extra1"
				},
				BasicFeatureTypes.DEFAULT_NAMESPACE,
				ClusteringUtils.CLUSTERING_CRS).getFeatureType();
		final GeometryFactory factory = new GeometryFactory();
		SimpleFeature feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"123",
				"fred",
				"NA",
				20.30203,
				factory.createPoint(new Coordinate(
						02.33,
						0.23)),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				1,
				1,
				0);
		assertEquals(
				new Coordinate(
						02.33,
						0.23),
				((Geometry) feature.getDefaultGeometry()).getCoordinate());
		System.out.println(((Geometry) feature.getDefaultGeometry()).getPrecisionModel());
		System.out.println(((Geometry) feature.getDefaultGeometry()).getEnvelope());

		feature = AnalyticFeature.createGeometryFeature(
				ftype,
				"b1",
				"123",
				"fred",
				"NA",
				20.30203,
				factory.createPoint(new Coordinate(
						02.33,
						0.23)),
				new String[] {
					"extra1"
				},
				new double[] {
					0.022
				},
				10,
				1,
				0);

		assertEquals(
				new Coordinate(
						02.33,
						0.23),
				((Geometry) feature.getDefaultGeometry()).getCoordinate());

		assertEquals(
				"geometry",
				feature.getFeatureType().getGeometryDescriptor().getName().getLocalPart());

		assertEquals(
				new Integer(
						10),
				feature.getAttribute(ClusterFeatureAttribute.ZOOM_LEVEL.attrName()));

		Filter gtFilter = ECQL.toFilter("BBOX(geometry,2,0,3,1) and level = 10");
		assertTrue(gtFilter.evaluate(feature));
		gtFilter = ECQL.toFilter("BBOX(geometry,2,0,3,1) and level = 9");
		assertFalse(gtFilter.evaluate(feature));
		gtFilter = ECQL.toFilter("BBOX(geometry,2,0,3,1) and batchID = 'b1'");
		assertTrue(gtFilter.evaluate(feature));

	}
}
