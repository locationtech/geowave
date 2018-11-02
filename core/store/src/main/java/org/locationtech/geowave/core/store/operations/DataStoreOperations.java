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
package org.locationtech.geowave.core.store.operations;

import java.io.IOException;

import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.Index;

public interface DataStoreOperations
{

	boolean indexExists(
			String indexName )
			throws IOException;

	boolean createIndex(
			Index index )
			throws IOException;

	boolean metadataExists(
			MetadataType type )
			throws IOException;

	void deleteAll()
			throws Exception;

	boolean deleteAll(
			String indexName,
			String typeName,
			Short adapterId,
			String... additionalAuthorizations );

	boolean ensureAuthorizations(
			String clientUser,
			String... authorizations );

	RowWriter createWriter(
			Index index,
			InternalDataAdapter<?> adapter );

	MetadataWriter createMetadataWriter(
			MetadataType metadataType );

	MetadataReader createMetadataReader(
			MetadataType metadataType );

	MetadataDeleter createMetadataDeleter(
			MetadataType metadataType );

	<T> RowReader<T> createReader(
			ReaderParams<T> readerParams );

	<T> Deleter<T> createDeleter(
			ReaderParams<T> readerParams );

	RowDeleter createRowDeleter(
			String indexName,
			PersistentAdapterStore adapterStore,
			InternalAdapterStore internalAdapterStore,
			String... authorizations );

	boolean mergeData(
			final Index index,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore );

	boolean mergeStats(
			DataStatisticsStore statsStore,
			InternalAdapterStore internalAdapterStore );
}
