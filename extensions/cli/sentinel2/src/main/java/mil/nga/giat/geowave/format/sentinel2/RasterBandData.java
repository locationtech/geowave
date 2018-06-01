/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.format.sentinel2;

import org.geotools.coverage.grid.GridCoverage2D;
import org.opengis.coverage.grid.GridCoverageReader;

/**
 * Contains attributes of a GridCoverage2D fetched from a Sentinel2 provider.
 */
public class RasterBandData
{
	public final String name;
	public final GridCoverage2D coverage;
	public final GridCoverageReader reader;
	public final double nodataValue;

	public RasterBandData(
			final String name,
			final GridCoverage2D coverage,
			final GridCoverageReader reader,
			final double nodataValue ) {
		this.name = name;
		this.coverage = coverage;
		this.reader = reader;
		this.nodataValue = nodataValue;
	}
}
