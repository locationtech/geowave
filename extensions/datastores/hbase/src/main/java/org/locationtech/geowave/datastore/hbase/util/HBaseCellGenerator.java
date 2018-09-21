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
package org.locationtech.geowave.datastore.hbase.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.data.VisibilityWriter;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;

/**
 * Functionality similar to <code> AccumuloKeyValuePairGenerator </code> Since
 * HBase already has a concept of Cell, we are using it rather than custom
 * implementation of KeyValue Pair
 */
public class HBaseCellGenerator<T>
{
	private final InternalDataAdapter<T> adapter;
	private final Index index;
	private final VisibilityWriter<T> visibilityWriter;

	public HBaseCellGenerator(
			final InternalDataAdapter<T> adapter,
			final Index index,
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
