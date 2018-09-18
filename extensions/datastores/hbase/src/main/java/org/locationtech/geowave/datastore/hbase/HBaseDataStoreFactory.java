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
package org.locationtech.geowave.datastore.hbase;

import org.locationtech.geowave.core.store.BaseDataStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryHelper;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.datastore.hbase.cli.config.HBaseOptions;
import org.locationtech.geowave.datastore.hbase.cli.config.HBaseRequiredOptions;
import org.locationtech.geowave.datastore.hbase.operations.HBaseOperations;

public class HBaseDataStoreFactory extends
		BaseDataStoreFactory
{
	public HBaseDataStoreFactory(
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
		if (!(options instanceof HBaseRequiredOptions)) {
			throw new AssertionError(
					"Expected " + HBaseRequiredOptions.class.getSimpleName());
		}
		final HBaseRequiredOptions opts = (HBaseRequiredOptions) options;
		if (opts.getStoreOptions() == null) {
			opts.setStoreOptions(new HBaseOptions());
		}

		final DataStoreOperations hbaseOperations = helper.createOperations(opts);

		return new HBaseDataStore(
				(HBaseOperations) hbaseOperations,
				(HBaseOptions) opts.getStoreOptions());
	}
}
