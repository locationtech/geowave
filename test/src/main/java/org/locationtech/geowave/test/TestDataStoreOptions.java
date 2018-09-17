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
package org.locationtech.geowave.test;

import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

public class TestDataStoreOptions extends
		DataStorePluginOptions
{
	private final GeoWaveStoreType storeType;

	public TestDataStoreOptions(
			final GeoWaveStoreType storeType ) {
		super();
		this.storeType = storeType;
	}

	public GeoWaveStoreType getStoreType() {
		return storeType;
	}
}
