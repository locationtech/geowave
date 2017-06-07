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
package mil.nga.giat.geowave.adapter.raster.adapter;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

import org.opengis.coverage.grid.GridCoverage;

public class RasterTileWriter implements
		FieldWriter<GridCoverage, RasterTile<?>>
{
	@Override
	public byte[] getVisibility(
			final GridCoverage rowValue,
			final ByteArrayId fieldId,
			final RasterTile<?> fieldValue ) {
		return new byte[] {};
	}

	@Override
	public byte[] writeField(
			final RasterTile<?> fieldValue ) {
		// there is no need to preface the payload with the class name and a
		// length of the class name, the implementation is assumed to be known
		// on read so we can save space on persistence
		return fieldValue.toBinary();
	}

}
