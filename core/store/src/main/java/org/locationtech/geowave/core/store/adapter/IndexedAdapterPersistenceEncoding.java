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

import java.util.Map.Entry;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;

import java.util.Set;

/**
 * This is an implements of persistence encoding that also contains all of the
 * extended data values used to form the native type supported by this adapter.
 * It also contains information about the persisted object within a particular
 * index such as the insertion ID in the index and the number of duplicates for
 * this entry in the index, and is used when reading data from the index.
 */
public class IndexedAdapterPersistenceEncoding extends
		AbstractAdapterPersistenceEncoding
{
	public IndexedAdapterPersistenceEncoding(
			final short adapterId,
			final ByteArray dataId,
			final ByteArray partitionKey,
			final ByteArray sortKey,
			final int duplicateCount,
			final PersistentDataset<CommonIndexValue> commonData,
			final PersistentDataset<byte[]> unknownData,
			final PersistentDataset<Object> adapterExtendedData ) {
		super(
				adapterId,
				dataId,
				partitionKey,
				sortKey,
				duplicateCount,
				commonData,
				unknownData,
				adapterExtendedData);
	}

	@Override
	public void convertUnknownValues(
			final DataTypeAdapter<?> adapter,
			final CommonIndexModel model ) {
		final Set<Entry<String, byte[]>> unknownDataValues = getUnknownData().getValues().entrySet();
		for (final Entry<String, byte[]> v : unknownDataValues) {
			final FieldReader<Object> reader = adapter.getReader(v.getKey());
			final Object value = reader.readField(v.getValue());
			adapterExtendedData.addValue(
					v.getKey(),
					value);
		}
	}
}
