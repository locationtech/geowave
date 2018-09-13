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
/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase;

import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.InternalAdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.SecondaryIndexStoreImpl;
import mil.nga.giat.geowave.core.store.server.ServerOpHelper;
import mil.nga.giat.geowave.core.store.server.ServerSideOperations;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.operations.HBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.server.RowMergingServerOp;
import mil.nga.giat.geowave.datastore.hbase.server.RowMergingVisibilityServerOp;
import mil.nga.giat.geowave.mapreduce.BaseMapReduceDataStore;

public class HBaseDataStore extends
		BaseMapReduceDataStore
{
	public HBaseDataStore(
			final HBaseOperations operations,
			final HBaseOptions options ) {
		this(
				new IndexStoreImpl(
						operations,
						options),
				new AdapterStoreImpl(
						operations,
						options),
				new DataStatisticsStoreImpl(
						operations,
						options),
				new AdapterIndexMappingStoreImpl(
						operations,
						options),
				new SecondaryIndexStoreImpl(),
				operations,
				options,
				new InternalAdapterStoreImpl(
						operations));
	}

	public HBaseDataStore(
			final IndexStore indexStore,
			final PersistentAdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final HBaseOperations operations,
			final HBaseOptions options,
			final InternalAdapterStore adapterMappingStore ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				secondaryIndexDataStore,
				operations,
				options,
				adapterMappingStore);

		secondaryIndexDataStore.setDataStore(this);
	}

	@Override
	protected <T> void initOnIndexWriterCreate(
			final InternalDataAdapter<T> adapter,
			final PrimaryIndex index ) {
		final String indexName = index.getId().getString();
		final boolean rowMerging = adapter.getAdapter() instanceof RowMergingDataAdapter;
		if (rowMerging) {
			if (!((HBaseOperations) baseOperations).isRowMergingEnabled(
					adapter.getInternalAdapterId(),
					indexName)) {
				if (baseOptions.isCreateTable()) {
					((HBaseOperations) baseOperations).createTable(
							index.getIndexStrategy().getPredefinedSplits(),
							index.getId(),
							false,
							adapter.getInternalAdapterId());
				}
				if (baseOptions.isServerSideLibraryEnabled()) {
					((HBaseOperations) baseOperations).ensureServerSideOperationsObserverAttached(index.getId());
					ServerOpHelper.addServerSideRowMerging(
							((RowMergingDataAdapter<?, ?>) adapter.getAdapter()),
							adapter.getInternalAdapterId(),
							(ServerSideOperations) baseOperations,
							RowMergingServerOp.class.getName(),
							RowMergingVisibilityServerOp.class.getName(),
							indexName);
				}

				((HBaseOperations) baseOperations).verifyColumnFamily(
						adapter.getInternalAdapterId(),
						false,
						indexName,
						true);
			}
		}
	}
}
