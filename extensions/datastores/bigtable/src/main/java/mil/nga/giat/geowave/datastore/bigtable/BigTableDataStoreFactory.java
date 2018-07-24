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
package mil.nga.giat.geowave.datastore.bigtable;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.InternalAdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.SecondaryIndexStoreImpl;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.datastore.bigtable.operations.BigTableOperations;
import mil.nga.giat.geowave.datastore.bigtable.operations.config.BigTableOptions;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStore;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.operations.HBaseOperations;

public class BigTableDataStoreFactory extends
		DataStoreFactory
{
	public BigTableDataStoreFactory(
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
		if (!(options instanceof BigTableOptions)) {
			throw new AssertionError(
					"Expected " + BigTableOptions.class.getSimpleName());
		}

		final BigTableOperations bigtableOperations = (BigTableOperations) helper.createOperations(options);

		HBaseOptions hbaseOptions = ((BigTableOptions) options).getHBaseOptions();
		// make sure to explicitly use the constructor with
		// BigTableDataStatisticsStore
		return new HBaseDataStore(
				new IndexStoreImpl(
						bigtableOperations,
						hbaseOptions),
				new AdapterStoreImpl(
						bigtableOperations,
						hbaseOptions),
				new BigTableDataStatisticsStore(
						bigtableOperations,
						hbaseOptions),
				new AdapterIndexMappingStoreImpl(
						bigtableOperations,
						hbaseOptions),
				new SecondaryIndexStoreImpl(),
				bigtableOperations,
				hbaseOptions,
				new InternalAdapterStoreImpl(
						bigtableOperations));
	}
}
