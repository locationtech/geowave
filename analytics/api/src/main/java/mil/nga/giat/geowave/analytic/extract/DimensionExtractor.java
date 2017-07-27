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

import com.vividsolutions.jts.geom.Geometry;

/**
 * Strategy to extract a representative dimensions and Geometry for an Object
 * 
 * @param <T>
 */
public interface DimensionExtractor<T> extends
		java.io.Serializable
{
	/**
	 * 
	 * @param anObject
	 *            --
	 */
	public double[] getDimensions(
			T anObject );

	/**
	 * 
	 * @return Dimension names in the same order as dimentions returns from the
	 *         {@link DimensionExtractor#getDimensions(Object)}
	 */
	public String[] getDimensionNames();

	/**
	 * 
	 * @param anObject
	 *            -- an object with Geospatial properties
	 * @return A Point that must have the SRID set for a valid CRS.
	 */
	public Geometry getGeometry(
			T anObject );

	/**
	 * @param An
	 *            assigned group ID, if one exists. null, otherwisw. --
	 */
	public String getGroupID(
			T anObject );

}
