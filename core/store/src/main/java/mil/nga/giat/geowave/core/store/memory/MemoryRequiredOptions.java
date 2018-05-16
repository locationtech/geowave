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

import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;

/**
 * No additional options for memory.
 */
public class MemoryRequiredOptions extends
		StoreFactoryOptions
{
	private final DataStoreOptions options = new BaseDataStoreOptions() {

		@Override
		public boolean isServerSideLibraryEnabled() {
			// memory datastore doesn't have a serverside option
			return false;
		}

	};

	@Override
	public StoreFactoryFamilySpi getStoreFactory() {
		return new MemoryStoreFactoryFamily();
	}

	@Override
	public DataStoreOptions getStoreOptions() {
		return options;
	}
}
