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
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.Index;

public interface DataStoreOperations
{

	public boolean indexExists(
			String indexName )
			throws IOException;

	public boolean createIndex(
			Index index )
			throws IOException;

	public boolean metadataExists(
			MetadataType type )
			throws IOException;

	public void deleteAll()
			throws Exception;

	public boolean deleteAll(
			String indexName,
			Short adapterId,
			String... additionalAuthorizations );

	public boolean ensureAuthorizations(
			String clientUser,
			String... authorizations );

	/**
	 * Creates a new writer that can be used by an index.
	 * 
	 * @param typeName
	 *            TODO
	 * @param adapterId
	 *            The name of the adapter.
	 * @param indexId
	 *            The basic name of the table. Note that that basic
	 *            implementation of the factory will allow for a table namespace
	 *            to prefix this name
	 * @param options
	 *            basic options available
	 * @param splits
	 *            If the table is created, these splits will be added as
	 *            partition keys. Null can be used to imply not to add any
	 *            splits.
	 *
	 * @return The appropriate writer
	 * @throws TableNotFoundException
	 *             The table does not exist in this Accumulo instance
	 */
	public RowWriter createWriter(
			Index index,
			String typeName,
			short adapterId );

	public MetadataWriter createMetadataWriter(
			MetadataType metadataType );

	public MetadataReader createMetadataReader(
			MetadataType metadataType );

	public MetadataDeleter createMetadataDeleter(
			MetadataType metadataType );

	public <T> RowReader<T> createReader(
			ReaderParams<T> readerParams );

	public <T> Deleter<T> createDeleter(
			ReaderParams<T> readerParams );

	public RowDeleter createRowDeleter(
			String indexName,
			String... authorizations );

	public boolean mergeData(
			final Index index,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore );

	public boolean mergeStats(
			DataStatisticsStore statsStore,
			InternalAdapterStore internalAdapterStore );
}
