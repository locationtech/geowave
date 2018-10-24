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
package org.locationtech.geowave.adapter.vector;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.DataStore;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.locationtech.geowave.adapter.vector.plugin.GeoWaveGTDataStoreFactory;
import org.locationtech.geowave.adapter.vector.plugin.GeoWavePluginConfig;
import org.locationtech.geowave.adapter.vector.plugin.GeoWavePluginException;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;

public class BaseDataStoreTest
{
	@Rule
	public TestName name = new TestName();

	protected DataStore createDataStore()
			throws IOException,
			GeoWavePluginException {
		final Map<String, Serializable> params = new HashMap<>();
		params.put(
				"gwNamespace",
				"test_" + getClass().getName() + "_" + name.getMethodName());
		final StoreFactoryFamilySpi storeFactoryFamily = new MemoryStoreFactoryFamily();
		// delete existing data
		new GeoWavePluginConfig(
				storeFactoryFamily,
				params).getDataStore().delete(
				QueryBuilder.newBuilder().build());

		return new GeoWaveGTDataStoreFactory(
				storeFactoryFamily).createNewDataStore(params);
	}
}
