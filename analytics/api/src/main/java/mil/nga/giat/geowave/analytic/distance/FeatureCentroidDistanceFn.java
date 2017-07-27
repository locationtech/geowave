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
package mil.nga.giat.geowave.analytic.distance;

import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

/**
 * Calculate distance between two SimpleFeatures, assuming has a Geometry.
 * 
 * @see org.opengis.feature.simple.SimpleFeature
 * 
 */
public class FeatureCentroidDistanceFn implements
		DistanceFn<SimpleFeature>
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3824608959408031752L;
	private DistanceFn<Coordinate> coordinateDistanceFunction = new CoordinateEuclideanDistanceFn();

	public FeatureCentroidDistanceFn() {}

	public FeatureCentroidDistanceFn(
			final DistanceFn<Coordinate> coordinateDistanceFunction ) {
		super();
		this.coordinateDistanceFunction = coordinateDistanceFunction;
	}

	public DistanceFn<Coordinate> getCoordinateDistanceFunction() {
		return coordinateDistanceFunction;
	}

	public void setCoordinateDistanceFunction(
			final DistanceFn<Coordinate> coordinateDistanceFunction ) {
		this.coordinateDistanceFunction = coordinateDistanceFunction;
	}

	private Geometry getGeometry(
			SimpleFeature x ) {
		for (Object attr : x.getAttributes())
			if (attr instanceof Geometry) return (Geometry) attr;
		return (Geometry) x.getDefaultGeometry();
	}

	@Override
	public double measure(
			final SimpleFeature x,
			final SimpleFeature y ) {

		return coordinateDistanceFunction.measure(
				getGeometry(
						x).getCentroid().getCoordinate(),
				getGeometry(
						y).getCentroid().getCoordinate());
	}
}
