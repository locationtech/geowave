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
import org.locationtech.geowave.core.store.adapter.AdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.SecondaryIndexDataStore;
import org.locationtech.geowave.core.store.metadata.AdapterIndexMappingStoreFactory;
import org.locationtech.geowave.core.store.metadata.AdapterStoreFactory;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreFactory;
import org.locationtech.geowave.core.store.metadata.IndexStoreFactory;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreFactory;
import org.locationtech.geowave.core.store.metadata.SecondaryIndexStoreFactory;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.DataStoreOperationsFactory;

public class BaseDataStoreFamily implements
		StoreFactoryFamilySpi
{
	private final String typeName;
	private final String description;
	private final StoreFactoryHelper helper;

	public BaseDataStoreFamily(
			final String typeName,
			final String description,
			final StoreFactoryHelper helper ) {
		super();
		this.typeName = typeName;
		this.description = description;
		this.helper = helper;
	}

	@Override
	public String getType() {
		return typeName;
	}

	@Override
	public String getDescription() {
		return description;
	}

	@Override
	public GenericStoreFactory<DataStore> getDataStoreFactory() {
		return new BaseDataStoreFactory(
				typeName,
				description,
				helper);
	}

	@Override
	public GenericStoreFactory<DataStatisticsStore> getDataStatisticsStoreFactory() {
		return new DataStatisticsStoreFactory(
				typeName,
				description,
				helper);
	}

	@Override
	public GenericStoreFactory<IndexStore> getIndexStoreFactory() {
		return new IndexStoreFactory(
				typeName,
				description,
				helper);
	}

	@Override
	public GenericStoreFactory<PersistentAdapterStore> getAdapterStoreFactory() {
		return new AdapterStoreFactory(
				typeName,
				description,
				helper);
	}

	@Override
	public GenericStoreFactory<AdapterIndexMappingStore> getAdapterIndexMappingStoreFactory() {
		return new AdapterIndexMappingStoreFactory(
				typeName,
				description,
				helper);
	}

	@Override
	public GenericStoreFactory<SecondaryIndexDataStore> getSecondaryIndexDataStore() {
		return new SecondaryIndexStoreFactory(
				typeName,
				description,
				helper);
	}

	@Override
	public GenericStoreFactory<DataStoreOperations> getDataStoreOperationsFactory() {
		return new DataStoreOperationsFactory(
				typeName,
				description,
				helper);
	}

	@Override
	public GenericStoreFactory<InternalAdapterStore> getInternalAdapterStoreFactory() {
		return new InternalAdapterStoreFactory(
				typeName,
				description,
				helper);
	}

}
