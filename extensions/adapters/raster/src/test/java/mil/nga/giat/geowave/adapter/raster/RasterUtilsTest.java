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
package mil.nga.giat.geowave.adapter.raster;

import org.junit.Assert;
import org.junit.Test;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;

public class RasterUtilsTest
{
	@Test
	public void testCreateDataAdapter() {
		final RasterDataAdapter adapter = RasterUtils.createDataAdapterTypeDouble(
				"test",
				3,
				256,
				new NoDataMergeStrategy());
		Assert.assertNotNull(adapter);
		Assert.assertEquals(
				"test",
				adapter.getCoverageName());
		Assert.assertEquals(
				3,
				adapter.getSampleModel().getNumBands());
		Assert.assertEquals(
				256,
				adapter.getTileSize());
	}
}
