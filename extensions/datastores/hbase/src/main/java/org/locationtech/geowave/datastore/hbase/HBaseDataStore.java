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
package org.locationtech.geowave.datastore.hbase;

import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.PrimaryIndex;
import org.locationtech.geowave.core.store.index.SecondaryIndexDataStore;
import org.locationtech.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import org.locationtech.geowave.core.store.metadata.AdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;
import org.locationtech.geowave.core.store.metadata.IndexStoreImpl;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.SecondaryIndexStoreImpl;
import org.locationtech.geowave.core.store.server.ServerOpHelper;
import org.locationtech.geowave.core.store.server.ServerSideOperations;
import org.locationtech.geowave.datastore.hbase.cli.config.HBaseOptions;
import org.locationtech.geowave.datastore.hbase.operations.HBaseOperations;
import org.locationtech.geowave.datastore.hbase.server.RowMergingServerOp;
import org.locationtech.geowave.datastore.hbase.server.RowMergingVisibilityServerOp;
import org.locationtech.geowave.mapreduce.BaseMapReduceDataStore;

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
