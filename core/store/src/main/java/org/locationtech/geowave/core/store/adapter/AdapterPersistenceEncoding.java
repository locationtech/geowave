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
package org.locationtech.geowave.core.store.adapter;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;

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
			final ByteArray dataId,
			final PersistentDataset<CommonIndexValue> commonData,
			final PersistentDataset<Object> adapterExtendedData ) {
		this(
				(short) -1,
				dataId,
				commonData,
				adapterExtendedData);
	}

	public AdapterPersistenceEncoding(
			final short internalAdapterId,
			final ByteArray dataId,
			final PersistentDataset<CommonIndexValue> commonData,
			final PersistentDataset<Object> adapterExtendedData ) {
		super(
				internalAdapterId,
				dataId,
				null,
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
			final DataTypeAdapter<?> adapter,
			final CommonIndexModel model ) {
		// inherently no unknown data, nothing to do
	}

}
