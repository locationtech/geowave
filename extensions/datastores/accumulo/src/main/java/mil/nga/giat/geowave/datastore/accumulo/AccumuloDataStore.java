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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.InternalAdapterStoreImpl;
import mil.nga.giat.geowave.core.store.server.ServerOpHelper;
import mil.nga.giat.geowave.core.store.server.ServerSideOperations;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.AccumuloSplitsProvider;
import mil.nga.giat.geowave.datastore.accumulo.operations.AccumuloOperations;
import mil.nga.giat.geowave.mapreduce.BaseMapReduceDataStore;
import mil.nga.giat.geowave.mapreduce.splits.SplitsProvider;

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
		this(
				new IndexStoreImpl(
						accumuloOperations,
						accumuloOptions),
				new AdapterStoreImpl(
						accumuloOperations,
						accumuloOptions),
				new DataStatisticsStoreImpl(
						accumuloOperations,
						accumuloOptions),
				new AccumuloSecondaryIndexDataStore(
						accumuloOperations,
						accumuloOptions),
				new AdapterIndexMappingStoreImpl(
						accumuloOperations,
						accumuloOptions),
				accumuloOperations,
				accumuloOptions,
				new InternalAdapterStoreImpl(
						accumuloOperations));
	}

	public AccumuloDataStore(
			final IndexStore indexStore,
			final PersistentAdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AccumuloSecondaryIndexDataStore secondaryIndexDataStore,
			final AdapterIndexMappingStore indexMappingStore,
			final AccumuloOperations accumuloOperations,
			final InternalAdapterStore adapterMappingStore ) {
		this(
				indexStore,
				adapterStore,
				statisticsStore,
				secondaryIndexDataStore,
				indexMappingStore,
				accumuloOperations,
				new AccumuloOptions(),
				adapterMappingStore);
	}

	public AccumuloDataStore(
			final IndexStore indexStore,
			final PersistentAdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AccumuloSecondaryIndexDataStore secondaryIndexDataStore,
			final AdapterIndexMappingStore indexMappingStore,
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions,
			final InternalAdapterStore adapterMappingStore ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				secondaryIndexDataStore,
				accumuloOperations,
				accumuloOptions,
				adapterMappingStore);

		secondaryIndexDataStore.setDataStore(this);
	}

	@Override
	protected SplitsProvider createSplitsProvider() {
		return new AccumuloSplitsProvider();
	}

	@Override
	protected void initOnIndexWriterCreate(
			final InternalDataAdapter adapter,
			final PrimaryIndex index ) {

		final String indexName = index.getId().getString();

		try {
			if (adapter.getAdapter() instanceof RowMergingDataAdapter) {
				if (!((AccumuloOperations) baseOperations).isRowMergingEnabled(
						adapter.getInternalAdapterId(),
						indexName)) {
					if (baseOptions.isCreateTable()) {
						if (!((AccumuloOperations) baseOperations).createTable(
								indexName,
								false,
								baseOptions.isEnableBlockCache())) {
							((AccumuloOperations) baseOperations).enableVersioningIterator(
									indexName,
									false);
						}
					}
					else {
						((AccumuloOperations) baseOperations).enableVersioningIterator(
								indexName,
								false);
					}
					if (baseOptions.isServerSideLibraryEnabled()) {
						ServerOpHelper.addServerSideRowMerging(
								((RowMergingDataAdapter<?, ?>) adapter.getAdapter()),
								adapter.getInternalAdapterId(),
								(ServerSideOperations) baseOperations,
								RowMergingCombiner.class.getName(),
								RowMergingVisibilityCombiner.class.getName(),
								indexName);
					}
				}
			}

			final byte[] adapterId = adapter.getAdapterId().getBytes();
			if (((AccumuloOptions) baseOptions).isUseLocalityGroups()
					&& !((AccumuloOperations) baseOperations).localityGroupExists(
							indexName,
							adapterId)) {
				((AccumuloOperations) baseOperations).addLocalityGroup(
						indexName,
						adapterId);
			}
		}
		catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to determine existence of locality group [" + adapter.getAdapterId().getString() + "]",
					e);
		}

	}
}
