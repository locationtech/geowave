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
package mil.nga.giat.geowave.datastore.hbase.metadata;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

public class HBaseAdapterStore extends
		AbstractHBasePersistence<DataAdapter<?>> implements
		AdapterStore
{
	protected static final String ADAPTER_CF = "ADAPTER";

	public HBaseAdapterStore(
			BasicHBaseOperations operation ) {
		super(
				operation);
	}

	@Override
	public void addAdapter(
			DataAdapter<?> adapter ) {
		addObject(adapter);

	}

	@Override
	public DataAdapter<?> getAdapter(
			ByteArrayId adapterId ) {
		return getObject(
				adapterId,
				null);
	}

	@Override
	public boolean adapterExists(
			ByteArrayId adapterId ) {
		return objectExists(
				adapterId,
				null);
	}

	@Override
	public CloseableIterator<DataAdapter<?>> getAdapters() {
		return getObjects();
	}

	@Override
	protected ByteArrayId getPrimaryId(
			DataAdapter<?> persistedObject ) {
		return persistedObject.getAdapterId();
	}

	@Override
	protected String getPersistenceTypeName() {
		return ADAPTER_CF;
	}

}
