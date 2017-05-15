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
package mil.nga.giat.geowave.core.store.memory;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

public class MemorySecondaryIndexStoreFactory extends
		AbstractMemoryStoreFactory<SecondaryIndexDataStore>
{
	private static final Map<String, SecondaryIndexDataStore> STATISTICS_STORE_CACHE = new HashMap<String, SecondaryIndexDataStore>();

	@Override
	public SecondaryIndexDataStore createStore(
			StoreFactoryOptions configOptions ) {
		return createStore(configOptions.getGeowaveNamespace());
	}

	protected static SecondaryIndexDataStore createStore(
			final String namespace ) {
		SecondaryIndexDataStore store = STATISTICS_STORE_CACHE.get(namespace);
		if (store == null) {
			store = new MemorySecondaryIndexDataStore();
			STATISTICS_STORE_CACHE.put(
					namespace,
					store);
		}
		return store;
	}
}
