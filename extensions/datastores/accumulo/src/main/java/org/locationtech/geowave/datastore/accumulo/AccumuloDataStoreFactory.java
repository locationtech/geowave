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

import org.locationtech.geowave.core.store.BaseDataStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryHelper;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloRequiredOptions;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloOperations;

public class AccumuloDataStoreFactory extends
		BaseDataStoreFactory
{
	public AccumuloDataStoreFactory(
			final String typeName,
			final String description,
			final StoreFactoryHelper helper ) {
		super(
				typeName,
				description,
				helper);
	}

	@Override
	public DataStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof AccumuloRequiredOptions)) {
			throw new AssertionError(
					"Expected " + AccumuloRequiredOptions.class.getSimpleName());
		}
		final AccumuloRequiredOptions opts = (AccumuloRequiredOptions) options;
		if (opts.getStoreOptions() == null) {
			opts.setStoreOptions(new AccumuloOptions());
		}

		final DataStoreOperations accumuloOperations = helper.createOperations(opts);
		return new AccumuloDataStore(
				(AccumuloOperations) accumuloOperations,
				(AccumuloOptions) opts.getStoreOptions());

	}
}
