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
package mil.nga.giat.geowave.adapter.raster.plugin;

import java.awt.RenderingHints.Key;
import java.util.Map;

import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridFormatFactorySpi;

public class GeoWaveGTRasterFormatFactory implements
		GridFormatFactorySpi
{

	@Override
	public boolean isAvailable() {
		return true;
	}

	@Override
	public Map<Key, ?> getImplementationHints() {
		return null;
	}

	@Override
	public AbstractGridFormat createFormat() {
		return new GeoWaveGTRasterFormat();
	}

}
