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
package mil.nga.giat.geowave.datastore.hbase.util;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexUtils;

public class HBaseSecondaryIndexUtils
{

	/**
	 * Decodes a result a HBase secondary index scan back into the original type
	 * using the given data adapter
	 * 
	 * @param row
	 * @param columnFamily
	 * @param adapter
	 * @return
	 */
	public static <T> T decodeRow(
			final Result row,
			final byte[] columnFamily,
			final DataAdapter<T> adapter,
			final PrimaryIndex index ) {
		final NavigableMap<byte[], byte[]> qualifierToValueMap = row.getFamilyMap(columnFamily);
		final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();
		final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();
		final PersistentDataset<byte[]> unknownData = new PersistentDataset<byte[]>();
		ByteArrayId dataId = null;
		final Map<ByteArrayId, byte[]> fieldIdToValueMap = new HashMap<>();
		for (final Entry<byte[], byte[]> entry : qualifierToValueMap.entrySet()) {
			final byte[] cqBytes = entry.getKey();
			final String dataIdString = SecondaryIndexUtils.getDataId(cqBytes);
			if (dataId == null) {
				dataId = new ByteArrayId(
						dataIdString);
			}
			final ByteArrayId fieldId = SecondaryIndexUtils.getFieldId(cqBytes);
			final byte[] fieldValue = entry.getValue();
			fieldIdToValueMap.put(
					fieldId,
					fieldValue);
		}
		for (final Entry<ByteArrayId, byte[]> entry : fieldIdToValueMap.entrySet()) {
			final ByteArrayId fieldId = entry.getKey();
			final byte[] fieldValueBytes = entry.getValue();
			final FieldReader<? extends CommonIndexValue> indexFieldReader = index.getIndexModel().getReader(
					fieldId);
			if (indexFieldReader != null) {
				final CommonIndexValue indexValue = indexFieldReader.readField(fieldValueBytes);
				final PersistentValue<CommonIndexValue> val = new PersistentValue<CommonIndexValue>(
						fieldId,
						indexValue);
				commonData.addValue(val);
			}
			else {
				final FieldReader<?> fieldReader = adapter.getReader(fieldId);
				if (fieldReader != null) {
					final Object fieldValue = fieldReader.readField(fieldValueBytes);
					final PersistentValue<Object> val = new PersistentValue<Object>(
							fieldId,
							fieldValue);
					extendedData.addValue(val);
				}
				else {
					unknownData.addValue(new PersistentValue<byte[]>(
							fieldId,
							fieldValueBytes));
				}
			}
		}
		final IndexedAdapterPersistenceEncoding encodedData = new IndexedAdapterPersistenceEncoding(
				adapter.getAdapterId(),
				dataId,
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
