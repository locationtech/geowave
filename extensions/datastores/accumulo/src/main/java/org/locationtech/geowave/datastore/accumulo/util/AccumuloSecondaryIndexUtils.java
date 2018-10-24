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
package org.locationtech.geowave.datastore.accumulo.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.index.SecondaryIndexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloSecondaryIndexUtils
{

	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloSecondaryIndexUtils.class);

	/**
	 * Decodes an Accumulo key-value pair from the result of a secondary index
	 * scan back into the original type using the given data adapter
	 *
	 * @param key
	 * @param value
	 * @param adapter
	 * @param indexModel
	 * @return
	 */
	public static <T> T decodeRow(
			final Key key,
			final Value value,
			final InternalDataAdapter<T> adapter,
			final Index index ) {
		Map<Key, Value> rowMapping;
		try {
			rowMapping = WholeRowIterator.decodeRow(
					key,
					value);
		}
		catch (final IOException e) {
			LOGGER.error("Could not decode row from iterator. Ensure whole row iterators are being used.");
			return null;
		}
		final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<>();
		final PersistentDataset<Object> extendedData = new PersistentDataset<>();
		final PersistentDataset<byte[]> unknownData = new PersistentDataset<>();
		ByteArray dataId = null;
		final Map<String, byte[]> fieldIdToValueMap = new HashMap<>();
		for (final Entry<Key, Value> entry : rowMapping.entrySet()) {
			final byte[] cqBytes = entry.getKey().getColumnQualifierData().getBackingArray();
			final String dataIdString = SecondaryIndexUtils.getDataId(cqBytes);
			if (dataId == null) {
				dataId = new ByteArray(
						dataIdString);
			}
			final String fieldId = SecondaryIndexUtils.getFieldName(cqBytes);
			final byte[] fieldValue = entry.getValue().get();
			fieldIdToValueMap.put(
					fieldId,
					fieldValue);
		}
		for (final Entry<String, byte[]> entry : fieldIdToValueMap.entrySet()) {
			final String fieldId = entry.getKey();
			final byte[] fieldValueBytes = entry.getValue();
			final FieldReader<? extends CommonIndexValue> indexFieldReader = index.getIndexModel().getReader(
					fieldId);
			if (indexFieldReader != null) {
				final CommonIndexValue indexValue = indexFieldReader.readField(fieldValueBytes);
				commonData.addValue(
						fieldId,
						indexValue);
			}
			else {
				final FieldReader<?> fieldReader = adapter.getReader(fieldId);
				if (fieldReader != null) {
					final Object fieldValue = fieldReader.readField(fieldValueBytes);
					extendedData.addValue(
							fieldId,
							fieldValue);
				}
				else {
					unknownData.addValue(
							fieldId,
							fieldValueBytes);
				}
			}
		}
		final IndexedAdapterPersistenceEncoding encodedData = new IndexedAdapterPersistenceEncoding(
				adapter.getAdapterId(),
				dataId,
				null,
				null,
				0,
				commonData,
				unknownData,
				extendedData);
		return adapter.decode(
				encodedData,
				index);
	}
}
