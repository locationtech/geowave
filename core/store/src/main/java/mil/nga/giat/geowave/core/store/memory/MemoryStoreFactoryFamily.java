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

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

public class MemoryStoreFactoryFamily extends
		AbstractMemoryFactory implements
		StoreFactoryFamilySpi
{

	@Override
	public GenericStoreFactory<DataStore> getDataStoreFactory() {
		return new MemoryDataStoreFactory();
	}

	@Override
	public GenericStoreFactory<DataStatisticsStore> getDataStatisticsStoreFactory() {
		return new MemoryDataStatisticsStoreFactory();
	}

	@Override
	public GenericStoreFactory<IndexStore> getIndexStoreFactory() {
		return new MemoryIndexStoreFactory();
	}

	@Override
	public GenericStoreFactory<AdapterStore> getAdapterStoreFactory() {
		return new MemoryAdapterStoreFactory();
	}

	@Override
	public GenericStoreFactory<SecondaryIndexDataStore> getSecondaryIndexDataStore() {
		return new MemorySecondaryIndexStoreFactory();
	}

	@Override
	public GenericStoreFactory<AdapterIndexMappingStore> getAdapterIndexMappingStoreFactory() {
		return new MemoryAdapterIndexMappingStoreFactory();
	}

	@Override
	public GenericStoreFactory<DataStoreOperations> getDataStoreOperationsFactory() {
		return new MemoryOperationsFactory();
	}
}
