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
package mil.nga.giat.geowave.core.store;

import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

public interface StoreFactoryFamilySpi extends
		GenericFactory
{
	public GenericStoreFactory<DataStore> getDataStoreFactory();

	public GenericStoreFactory<DataStatisticsStore> getDataStatisticsStoreFactory();

	public GenericStoreFactory<IndexStore> getIndexStoreFactory();

	public GenericStoreFactory<AdapterStore> getAdapterStoreFactory();

	public GenericStoreFactory<AdapterIndexMappingStore> getAdapterIndexMappingStoreFactory();

	public GenericStoreFactory<SecondaryIndexDataStore> getSecondaryIndexDataStore();

	public GenericStoreFactory<DataStoreOperations> getDataStoreOperationsFactory();
}
