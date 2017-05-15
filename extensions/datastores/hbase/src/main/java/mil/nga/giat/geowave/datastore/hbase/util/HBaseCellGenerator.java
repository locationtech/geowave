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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

/**
 * Functionality similar to <code> AccumuloKeyValuePairGenerator </code> Since
 * HBase already has a concept of Cell, we are using it rather than custom
 * implementation of KeyValue Pair
 */
public class HBaseCellGenerator<T>
{

	private final WritableDataAdapter<T> adapter;
	private final PrimaryIndex index;
	private final VisibilityWriter<T> visibilityWriter;

	public HBaseCellGenerator(
			final WritableDataAdapter<T> adapter,
			final PrimaryIndex index,
			final VisibilityWriter<T> visibilityWriter ) {
		super();
		this.adapter = adapter;
		this.index = index;
		this.visibilityWriter = visibilityWriter;
	}

	public List<Cell> constructKeyValuePairs(
			final byte[] adapterId,
			final T entry ) {

		final List<Cell> keyValuePairs = new ArrayList<>();
		Cell cell;
		final DataStoreEntryInfo ingestInfo = DataStoreUtils.getIngestInfo(
				adapter,
				index,
				entry,
				visibilityWriter);
		final List<ByteArrayId> rowIds = ingestInfo.getRowIds();
		@SuppressWarnings("rawtypes")
		final List<FieldInfo<?>> fieldInfoList = ingestInfo.getFieldInfo();

		for (final ByteArrayId rowId : rowIds) {
			for (@SuppressWarnings("rawtypes")
			final FieldInfo fieldInfo : fieldInfoList) {
				cell = CellUtil.createCell(
						rowId.getBytes(),
						adapterId,
						fieldInfo.getDataValue().getId().getBytes(),
						System.currentTimeMillis(),
						KeyValue.Type.Put.getCode(),
						fieldInfo.getWrittenValue());
				keyValuePairs.add(cell);
			}
		}

		return keyValuePairs;
	}

}
