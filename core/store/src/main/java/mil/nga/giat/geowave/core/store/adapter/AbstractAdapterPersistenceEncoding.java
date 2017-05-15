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
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

/**
 * 
 * @since 0.9.1
 */
public abstract class AbstractAdapterPersistenceEncoding extends
		CommonIndexedPersistenceEncoding
{
	protected final PersistentDataset<Object> adapterExtendedData;

	public AbstractAdapterPersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final ByteArrayId indexInsertionId,
			final int duplicateCount,
			final PersistentDataset<CommonIndexValue> commonData,
			final PersistentDataset<byte[]> unknownData,
			final PersistentDataset<Object> adapterExtendedData ) {
		super(
				adapterId,
				dataId,
				indexInsertionId,
				duplicateCount,
				commonData,
				unknownData);
		this.adapterExtendedData = adapterExtendedData;
	}

	/**
	 * This returns a representation of the custom fields for the data adapter
	 * 
	 * @return the extended data beyond the common index fields that are
	 *         provided by the adapter
	 */
	public PersistentDataset<Object> getAdapterExtendedData() {
		return adapterExtendedData;
	}

	/**
	 * Process unknownData given adapter and model to convert to adapter
	 * extended values
	 * 
	 * @param adapter
	 * @param model
	 */
	abstract public void convertUnknownValues(
			final DataAdapter<?> adapter,
			final CommonIndexModel model );
}
