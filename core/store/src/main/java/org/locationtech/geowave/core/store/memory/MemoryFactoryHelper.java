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
package org.locationtech.geowave.core.store.memory;

import java.util.HashMap;
import java.util.Map;

import org.locationtech.geowave.core.store.StoreFactoryHelper;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;

public class MemoryFactoryHelper implements
		StoreFactoryHelper
{
	// this operations cache is essential to re-using the same objects in memory
	private static final Map<String, DataStoreOperations> OPERATIONS_CACHE = new HashMap<String, DataStoreOperations>();

	/**
	 * Return the default options instance. This is actually a method that
	 * should be implemented by the individual factories, but is placed here
	 * since it's the same.
	 *
	 * @return
	 */
	@Override
	public StoreFactoryOptions createOptionsInstance() {
		return new MemoryRequiredOptions();
	}

	@Override
	public DataStoreOperations createOperations(
			final StoreFactoryOptions options ) {
		synchronized (OPERATIONS_CACHE) {
			DataStoreOperations operations = OPERATIONS_CACHE.get(options.getGeowaveNamespace());
			if (operations == null) {
				operations = new MemoryDataStoreOperations(
						options.getStoreOptions());
				OPERATIONS_CACHE.put(
						options.getGeowaveNamespace(),
						operations);
			}
			return operations;
		}
	}
}
