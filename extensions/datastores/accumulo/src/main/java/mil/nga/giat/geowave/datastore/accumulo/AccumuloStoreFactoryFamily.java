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
package mil.nga.giat.geowave.datastore.accumulo;

import mil.nga.giat.geowave.core.store.BaseDataStoreFamily;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStoreFactory;

public class AccumuloStoreFactoryFamily extends
		BaseDataStoreFamily
{
	public final static String TYPE = "accumulo";
	private static final String DESCRIPTION = "A GeoWave store backed by tables in Apache Accumulo";

	public AccumuloStoreFactoryFamily() {
		super(
				TYPE,
				DESCRIPTION,
				new AccumuloFactoryHelper());
	}

	@Override
	public GenericStoreFactory<DataStore> getDataStoreFactory() {
		return new AccumuloDataStoreFactory(
				TYPE,
				DESCRIPTION,
				new AccumuloFactoryHelper());
	}

	@Override
	public GenericStoreFactory<SecondaryIndexDataStore> getSecondaryIndexDataStore() {
		return new AccumuloSecondaryIndexDataStoreFactory(
				TYPE,
				DESCRIPTION,
				new AccumuloFactoryHelper());
	}

}
