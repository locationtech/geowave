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
package mil.nga.giat.geowave.analytic.extract;

import com.vividsolutions.jts.geom.Point;

/**
 * Strategy to extract a representative centroid from some Geospatial object
 * 
 * @param <T>
 */
public interface CentroidExtractor<T>
{
	/**
	 * 
	 * @param anObject
	 *            -- an object with Geospatial properties
	 * @return A Point that must have the SRID set for a valid CRS.
	 */
	public Point getCentroid(
			T anObject );

}
