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
package mil.nga.giat.geowave.datastore.hbase.index.secondary;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.index.BaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.operations.HBaseOperations;

public class HBaseSecondaryIndexDataStore extends
		BaseSecondaryIndexDataStore
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseSecondaryIndexDataStore.class);
	private final BasicHBaseOperations hbaseOperations;
	@SuppressWarnings("unused")
	private final HBaseOptions hbaseOptions;
	private DataStore dataStore = null;

	public HBaseSecondaryIndexDataStore(
			final HBaseOperations hbaseOperations ) {
		this(
				hbaseOperations,
				new HBaseOptions());
	}

	public HBaseSecondaryIndexDataStore(
			final HBaseOperations hbaseOperations,
			final HBaseOptions hbaseOptions ) {
		super();
	}

	@Override
	public void setDataStore(
			final DataStore dataStore ) {}

	@Override
	public <T> CloseableIterator<T> query(
			final SecondaryIndex<T> secondaryIndex,
			final ByteArrayId indexedAttributeFieldId,
			final DataAdapter<T> adapter,
			final PrimaryIndex primaryIndex,
			final DistributableQuery query,
			final String... authorizations ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected GeoWaveRow buildJoinMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] primaryIndexId,
			final byte[] primaryIndexPartitionKey,
			final byte[] primaryIndexSortKey,
			final byte[] attributeVisibility )
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected GeoWaveRow buildMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] dataId,
			final byte[] fieldId,
			final byte[] fieldValue,
			final byte[] fieldVisibility )
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected GeoWaveRow buildJoinDeleteMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] primaryIndexId,
			final byte[] primaryIndexRowId )
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected GeoWaveRow buildFullDeleteMutation(
			final byte[] secondaryIndexRowId,
			final byte[] adapterId,
			final byte[] indexedAttributeFieldId,
			final byte[] dataId,
			final byte[] fieldId )
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Writer getWriter(
			final ByteArrayId secondaryIndexId ) {
		// TODO Auto-generated method stub
		return null;
	}

}
