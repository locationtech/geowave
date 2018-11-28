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
package org.locationtech.geowave.format.stanag4676.parser.model;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import java.util.ArrayList;
import java.util.List;

public class Area
{

	/**
	 * points is an ordered list defining an area 3 or more points define a
	 * polygon 2 points define a circle, the first being the center of the
	 * circle and the second being a point along the circumference. The radius
	 * of the circle would be the distance between the two points.
	 * 
	 */
	public List<GeodeticPosition> points = new ArrayList<GeodeticPosition>();

	public List<GeodeticPosition> getPoints() {
		return this.points;
	}

	public void setPoints(
			List<GeodeticPosition> points ) {
		this.points = points;
	}

	public Polygon getPolygon() {
		Polygon polygon = null;
		if (points.size() > 2) {
			Coordinate[] coords = new Coordinate[points.size() + 1];
			int c = 0;
			for (GeodeticPosition pos : points) {
				Coordinate coord = new Coordinate(
						pos.longitude,
						pos.latitude);
				coords[c] = coord;
				// Make sure the polygon is closed
				if (c == 0) coords[points.size()] = coord;
				c++;
			}
			GeometryFactory gf = new GeometryFactory();
			polygon = gf.createPolygon(coords);
		}
		return polygon;
	}

}
