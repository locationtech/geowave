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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import org.locationtech.geowave.core.store.metadata.AdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;
import org.locationtech.geowave.core.store.metadata.IndexStoreImpl;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.core.store.server.ServerOpHelper;
import org.locationtech.geowave.core.store.server.ServerSideOperations;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import org.locationtech.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import org.locationtech.geowave.datastore.accumulo.mapreduce.AccumuloSplitsProvider;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloOperations;
import org.locationtech.geowave.mapreduce.BaseMapReduceDataStore;
import org.locationtech.geowave.mapreduce.splits.SplitsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the Accumulo implementation of the data store. It requires an
 * AccumuloOperations instance that describes how to connect (read/write data)
 * to Apache Accumulo. It can create default implementations of the IndexStore
 * and AdapterStore based on the operations which will persist configuration
 * information to Accumulo tables, or an implementation of each of these stores
 * can be passed in A DataStore can both ingest and query data based on
 * persisted indices and data adapters. When the data is ingested it is
 * explicitly given an index and a data adapter which is then persisted to be
 * used in subsequent queries.
 */
public class AccumuloDataStore extends
		BaseMapReduceDataStore
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloDataStore.class);

	public AccumuloDataStore(
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions ) {
		super(
				new IndexStoreImpl(
						accumuloOperations,
						accumuloOptions),
				new AdapterStoreImpl(
						accumuloOperations,
						accumuloOptions),
				new DataStatisticsStoreImpl(
						accumuloOperations,
						accumuloOptions),
				new AdapterIndexMappingStoreImpl(
						accumuloOperations,
						accumuloOptions),
				new AccumuloSecondaryIndexDataStore(
						accumuloOperations,
						accumuloOptions),
				accumuloOperations,
				accumuloOptions,
				new InternalAdapterStoreImpl(
						accumuloOperations));
	}

	@Override
	protected SplitsProvider createSplitsProvider() {
		return new AccumuloSplitsProvider();
	}

	@Override
	protected void initOnIndexWriterCreate(
			final InternalDataAdapter adapter,
			final Index index ) {
		final String indexName = index.getName();
		final String typeName = adapter.getTypeName();
		try {
			if (adapter.getAdapter() instanceof RowMergingDataAdapter) {
				if (!((AccumuloOperations) baseOperations).isRowMergingEnabled(
						adapter.getAdapterId(),
						indexName)) {
					if (!((AccumuloOperations) baseOperations).createTable(
							indexName,
							false,
							baseOptions.isEnableBlockCache())) {
						((AccumuloOperations) baseOperations).enableVersioningIterator(
								indexName,
								false);
					}
					if (baseOptions.isServerSideLibraryEnabled()) {
						ServerOpHelper.addServerSideRowMerging(
								((RowMergingDataAdapter<?, ?>) adapter.getAdapter()),
								adapter.getAdapterId(),
								(ServerSideOperations) baseOperations,
								RowMergingCombiner.class.getName(),
								RowMergingVisibilityCombiner.class.getName(),
								indexName);
					}
				}
			}
			if (((AccumuloOptions) baseOptions).isUseLocalityGroups()
					&& !((AccumuloOperations) baseOperations).localityGroupExists(
							indexName,
							adapter.getTypeName())) {
				((AccumuloOperations) baseOperations).addLocalityGroup(
						indexName,
						adapter.getTypeName(),
						adapter.getAdapterId());
			}
		}
		catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to determine existence of locality group [" + typeName + "]",
					e);
		}

	}
}
