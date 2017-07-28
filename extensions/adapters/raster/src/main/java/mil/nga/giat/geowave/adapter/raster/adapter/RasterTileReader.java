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

import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;

public class RasterTileReader implements
		FieldReader<RasterTile<?>>
{

	@Override
	public RasterTile<?> readField(
			final byte[] fieldData ) {

		// the class name is not prefaced in the payload, we are assuming it is
		// a raster tile implementation and instantiating it directly

		final RasterTile retVal = new RasterTile();
		if (retVal != null) {
			retVal.fromBinary(fieldData);
		}
		return retVal;
	}

}
