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
package org.locationtech.geowave.core.geotime.ingest;

import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.spi.DimensionalityTypeOptions;

import com.beust.jcommander.Parameter;

public class SpatialOptions implements
		DimensionalityTypeOptions
{
	@Parameter(names = {
		"--storeTime"
	}, required = false, description = "The index will store temporal values.  This allows it to slightly more efficiently run spatial-temporal queries although if spatial-temporal queries are a common use case, a separate spatial-temporal index is recommended.")
	protected boolean storeTime = false;

	@Parameter(names = {
		"-c",
		"--crs"
	}, required = false, description = "The native Coordinate Reference System used within the index.  All spatial data will be projected into this CRS for appropriate indexing as needed.")
	protected String crs = GeometryUtils.DEFAULT_CRS_STR;

	public void setCrs(
			String crs ) {
		this.crs = crs;
	}
}
