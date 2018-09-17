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
package org.locationtech.geowave.datastore.accumulo;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;
import org.locationtech.geowave.core.store.server.RowMergingAdapterOptionProvider;
import org.locationtech.geowave.mapreduce.URLClassloaderUtils;

public class RowMergingCombiner extends
		MergingCombiner
{
	private RowTransform<Mergeable> rowTransform;

	@Override
	protected Mergeable getMergeable(
			final Key key,
			final byte[] binary ) {
		return rowTransform.getRowAsMergeableObject(
				ByteArrayUtils.shortFromString(key.getColumnFamily().toString()),
				new ByteArrayId(
						key.getColumnQualifier().getBytes()),
				binary);
	}

	@Override
	protected String getColumnOptionValue(
			final Map<String, String> options ) {
		// if this is "row" merging than it is by adapter ID
		return options.get(RowMergingAdapterOptionProvider.ADAPTER_IDS_OPTION);
	}

	@Override
	protected byte[] getBinary(
			final Mergeable mergeable ) {
		return rowTransform.getBinaryFromMergedObject(mergeable);
	}

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		super.init(
				source,
				options,
				env);
		final String rowTransformStr = options.get(RowMergingAdapterOptionProvider.ROW_TRANSFORM_KEY);
		final byte[] rowTransformBytes = ByteArrayUtils.byteArrayFromString(rowTransformStr);
		rowTransform = (RowTransform<Mergeable>) URLClassloaderUtils.fromBinary(rowTransformBytes);
		rowTransform.initOptions(options);
	}

}
