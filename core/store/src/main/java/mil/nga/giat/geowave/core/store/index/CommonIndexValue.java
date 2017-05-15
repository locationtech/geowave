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
package mil.nga.giat.geowave.core.store.index;

import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;

/**
 * A common index value can be very generic but must have a way to identify its
 * visibility
 * 
 */
public interface CommonIndexValue
{
	public byte[] getVisibility();

	public void setVisibility(
			byte[] visibility );

	public boolean overlaps(
			NumericDimensionField[] field,
			NumericData[] rangeData );
}
