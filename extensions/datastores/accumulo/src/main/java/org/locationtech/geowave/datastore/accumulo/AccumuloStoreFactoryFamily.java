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
package org.locationtech.geowave.datastore.accumulo;

import org.locationtech.geowave.core.store.BaseDataStoreFamily;
import org.locationtech.geowave.core.store.GenericStoreFactory;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.index.SecondaryIndexDataStore;
import org.locationtech.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStoreFactory;

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
