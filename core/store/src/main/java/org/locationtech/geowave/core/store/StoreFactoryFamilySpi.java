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
package org.locationtech.geowave.core.store;

import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.SecondaryIndexDataStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;

public interface StoreFactoryFamilySpi extends
		GenericFactory
{
	public GenericStoreFactory<DataStore> getDataStoreFactory();

	public GenericStoreFactory<DataStatisticsStore> getDataStatisticsStoreFactory();

	public GenericStoreFactory<IndexStore> getIndexStoreFactory();

	public GenericStoreFactory<PersistentAdapterStore> getAdapterStoreFactory();

	public GenericStoreFactory<AdapterIndexMappingStore> getAdapterIndexMappingStoreFactory();

	public GenericStoreFactory<InternalAdapterStore> getInternalAdapterStoreFactory();

	public GenericStoreFactory<SecondaryIndexDataStore> getSecondaryIndexDataStore();

	public GenericStoreFactory<DataStoreOperations> getDataStoreOperationsFactory();
}
