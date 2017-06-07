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

import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;

public class MemoryOperationsFactory extends
		AbstractMemoryStoreFactory<DataStoreOperations>
{
	private static final Map<String, DataStoreOperations> OPERATIONS_CACHE = new HashMap<String, DataStoreOperations>();

	@Override
	public DataStoreOperations createStore(
			final StoreFactoryOptions configOptions ) {
		return createStore(configOptions.getGeowaveNamespace());
	}

	protected static DataStoreOperations createStore(
			final String namespace ) {
		DataStoreOperations operations = OPERATIONS_CACHE.get(namespace);
		if (operations == null) {
			operations = new MemoryStoreOperations();
			OPERATIONS_CACHE.put(
					namespace,
					operations);
		}
		return operations;
	}
}
