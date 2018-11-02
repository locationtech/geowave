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
package org.locationtech.geowave.analytic.extract;

import org.locationtech.geowave.adapter.vector.FeatureGeometryHandler;
import org.opengis.feature.simple.SimpleFeature;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

/**
 * 
 * Extract a set of points representing critical points for a simple feature
 * that me be representative or compared to centroids.
 * 
 */
public class SimpleFeatureCentroidExtractor implements
		CentroidExtractor<SimpleFeature>
{
	@Override
	public Point getCentroid(
			final SimpleFeature anObject ) {
		final FeatureGeometryHandler handler = new FeatureGeometryHandler(
				anObject.getDefaultGeometryProperty().getDescriptor());
		final Geometry geometry = handler.toIndexValue(
				anObject).getGeometry();
		final int srid = SimpleFeatureGeometryExtractor.getSRID(anObject);
		final Point point = geometry.getCentroid();
		point.setSRID(srid);
		return point;
	}
}
