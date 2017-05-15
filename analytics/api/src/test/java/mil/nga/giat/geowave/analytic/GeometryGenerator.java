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

import java.util.Iterator;
import java.util.List;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateList;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class GeometryGenerator
{
	public static interface DistortationFn
	{
		double distort();
	}

	/**
	 * @param count
	 * @param distanceactors
	 * @param distortationFn
	 * @param delta
	 * @param env
	 * @return
	 */
	public static Iterator<Geometry> generate(
			final int count,
			final List<Double> distanceactors,
			final DistortationFn distortationFn,
			final double delta,
			final Envelope env ) {
		// Create the star-ellipses for intersections later on
		return new Iterator<Geometry>() {
			int currentCount = 0;
			GeometryFactory geometryFactory = new GeometryFactory();

			@Override
			public boolean hasNext() {
				return currentCount < count;
			}

			@Override
			public Geometry next() {
				// Thanks to Chris Bennight for the foundations of this code.
				currentCount++;
				double cx = env.centre().x * distortationFn.distort();
				double cy = env.centre().y * distortationFn.distort();

				double dx = env.getWidth() * distortationFn.distort();
				double dy = env.getHeight() * distortationFn.distort();

				// We will use a coordinate list to build the linear ring
				CoordinateList clist = new CoordinateList();
				double angle = 0.0;
				for (int i = 0; angle < 360; angle += delta * distortationFn.distort() + delta, i++) {
					double a = distanceactors.get(i % distanceactors.size()) * dx * distortationFn.distort();
					// double b = distanceactors.get(i % distanceactors.size())
					// * dy * distortationFn.distort();
					clist.add(new Coordinate(
							cx + a * Math.sin(Math.toRadians(angle)),
							cy + a * Math.cos(Math.toRadians(angle))));

				}

				clist.add(clist.get(0));
				return geometryFactory.createPolygon(clist.toCoordinateArray());
			}

			@Override
			public void remove() {

			}
		};
	}
}
