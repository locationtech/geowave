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
package mil.nga.giat.geowave.analytic.mapreduce.kmeans;

import mil.nga.giat.geowave.analytic.distance.CoordinateEuclideanDistanceFn;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class TestObjectDistanceFn implements
		DistanceFn<TestObject>
{
	private DistanceFn<Coordinate> coordinateDistanceFunction = new CoordinateEuclideanDistanceFn();

	private Geometry getGeometry(
			TestObject x ) {
		return x.geo;
	}

	@Override
	public double measure(
			TestObject x,
			TestObject y ) {

		return coordinateDistanceFunction.measure(
				getGeometry(
						x).getCentroid().getCoordinate(),
				getGeometry(
						y).getCentroid().getCoordinate());
	}

}
