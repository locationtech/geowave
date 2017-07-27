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

import mil.nga.giat.geowave.core.store.BaseDataStoreFamily;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

public class MemoryStoreFactoryFamily extends
		BaseDataStoreFamily implements
		StoreFactoryFamilySpi
{
	private static final String TYPE = "memory";
	private static final String DESCRIPTION = "A GeoWave store that is in memory typically only used for test purposes";

	public MemoryStoreFactoryFamily() {
		super(
				TYPE,
				DESCRIPTION,
				new MemoryFactoryHelper());
	}
	@Override
	public GenericStoreFactory<DataStoreOperations> getDataStoreOperationsFactory() {
		return new MemoryOperationsFactory();
	}
}
