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

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Mutation;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.data.VisibilityWriter;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloWriter;

/**
 *
 * Given a {@link DataTypeAdapter} and an {@link Index}, this class handles the
 * creation of Geowave-formatted [Key,Value] pairs.
 *
 * The intent is that this class will be used within the Mapper of a MapReduce
 * job to generate Keys and Values to be sorted during the shuffle-and-sort
 * phase in preparation for bulk ingest into Accumulo via
 * {@link AccumuloFileOutputFormat}.
 *
 * @param <T>
 *            the type of entries to index within Geowave
 */
public class AccumuloKeyValuePairGenerator<T>
{

	private final InternalDataAdapter<T> adapter;
	private final Index index;
	private final VisibilityWriter<T> visibilityWriter;

	public AccumuloKeyValuePairGenerator(
			final InternalDataAdapter<T> adapter,
			final Index index,
			final VisibilityWriter<T> visibilityWriter ) {
		super();
		this.adapter = adapter;
		this.index = index;
		this.visibilityWriter = visibilityWriter;
	}

	public List<KeyValue> constructKeyValuePairs(
			final T entry ) {
		final List<KeyValue> keyValuePairs = new ArrayList<>();
		final GeoWaveRow[] rows = BaseDataStoreUtils.getGeoWaveRows(
				entry,
				adapter,
				index,
				visibilityWriter);
		if ((rows != null) && (rows.length > 0)) {
			for (final GeoWaveRow row : rows) {
				final Mutation m = AccumuloWriter.rowToMutation(row);
				for (final ColumnUpdate cu : m.getUpdates()) {
					keyValuePairs.add(new KeyValue(
							new Key(
									m.getRow(),
									cu.getColumnFamily(),
									cu.getColumnQualifier(),
									cu.getColumnVisibility(),
									cu.getTimestamp()),
							cu.getValue()));
				}
			}
		}

		return keyValuePairs;
	}
}
