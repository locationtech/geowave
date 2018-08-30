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

import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.base.BaseDataStoreUtils;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * Functionality similar to <code> AccumuloKeyValuePairGenerator </code> Since
 * HBase already has a concept of Cell, we are using it rather than custom
 * implementation of KeyValue Pair
 */
public class HBaseCellGenerator<T>
{
	private final InternalDataAdapter<T> adapter;
	private final PrimaryIndex index;
	private final VisibilityWriter<T> visibilityWriter;

	public HBaseCellGenerator(
			final InternalDataAdapter<T> adapter,
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
		final GeoWaveRow[] rows = BaseDataStoreUtils.getGeoWaveRows(
				entry,
				adapter,
				index,
				visibilityWriter);

		if ((rows != null) && (rows.length > 0)) {
			for (final GeoWaveRow row : rows) {
				for (final GeoWaveValue value : row.getFieldValues()) {
					Cell cell = CellUtil.createCell(
							GeoWaveKey.getCompositeId(row),
							adapterId,
							row.getDataId(),
							System.currentTimeMillis(),
							KeyValue.Type.Put.getCode(),
							value.getValue());

					keyValuePairs.add(cell);
				}
			}
		}

		return keyValuePairs;
	}

}
