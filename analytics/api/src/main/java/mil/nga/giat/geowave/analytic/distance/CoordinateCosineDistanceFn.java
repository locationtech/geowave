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

public class CoordinateCosineDistanceFn implements
		DistanceFn<Coordinate>
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2074200104626591273L;

	@Override
	public double measure(
			final Coordinate x,
			final Coordinate y ) {
		final double ab = (x.x * y.x) + (x.y * y.y) + (x.z * y.z);
		final double norma = Math.sqrt(Math.pow(
				x.x,
				2) + Math.pow(
				x.y,
				2) + Math.pow(
				x.z,
				2));
		final double normb = Math.sqrt(Math.pow(
				y.x,
				2) + Math.pow(
				y.y,
				2) + Math.pow(
				y.z,
				2));
		return ab / (norma * normb);
	}

}
