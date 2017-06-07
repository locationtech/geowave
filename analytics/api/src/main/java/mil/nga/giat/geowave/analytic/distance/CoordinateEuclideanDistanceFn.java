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

import com.vividsolutions.jts.geom.Coordinate;

public class CoordinateEuclideanDistanceFn implements
		DistanceFn<Coordinate>
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 888639577783179566L;

	@Override
	public double measure(
			final Coordinate x,
			final Coordinate y ) {
		return Math.sqrt(Math.pow(
				(x.x - y.x),
				2) + Math.pow(
				(x.y - y.y),
				2) + Math.pow(
				(filter(x.z) - filter(y.z)),
				2));
	}

	private static double filter(
			final double x ) {
		return (Double.isNaN(x)) ? 0 : x;
	}

}
