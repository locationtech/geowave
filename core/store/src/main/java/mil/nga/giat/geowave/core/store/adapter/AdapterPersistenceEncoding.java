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
package mil.nga.giat.geowave.core.store.adapter;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

/**
 * This is an implementation of persistence encoding that also contains all of
 * the extended data values used to form the native type supported by this
 * adapter. It does not contain any information about the entry in a particular
 * index and is used when writing an entry, prior to its existence in an index.
 */
public class AdapterPersistenceEncoding extends
		AbstractAdapterPersistenceEncoding
{
	public AdapterPersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final PersistentDataset<CommonIndexValue> commonData,
			final PersistentDataset<Object> adapterExtendedData ) {
		super(
				adapterId,
				dataId,
				null,
				0,
				commonData,
				new PersistentDataset<byte[]>(),
				adapterExtendedData); // all data is identified by
										// the adapter, there is
										// inherently no unknown
										// data elements
	}

	@Override
	public void convertUnknownValues(
			final DataAdapter<?> adapter,
			final CommonIndexModel model ) {
		// inherently no unknown data, nothing to do
	}

}
