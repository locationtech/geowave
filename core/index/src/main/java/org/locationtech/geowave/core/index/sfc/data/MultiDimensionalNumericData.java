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
package org.locationtech.geowave.core.index.sfc.data;

import org.locationtech.geowave.core.index.IndexConstraints;
import org.locationtech.geowave.core.index.persist.Persistable;

/**
 * Interface which defines the methods associated with a multi-dimensional
 * numeric data range.
 * 
 */
public interface MultiDimensionalNumericData extends
		IndexConstraints,
		Persistable
{
	/**
	 * @return an array of object QueryRange
	 */
	public NumericData[] getDataPerDimension();

	public double[] getMaxValuesPerDimension();

	public double[] getMinValuesPerDimension();

	public double[] getCentroidPerDimension();
}
