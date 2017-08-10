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
package mil.nga.giat.geowave.datastore.accumulo;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

public class RowMergingCombiner extends
		MergingCombiner
{
	private RowTransform<Mergeable> rowTransform;

	@Override
	protected Mergeable getMergeable(
			final Key key,
			final byte[] binary ) {
		return rowTransform.getRowAsMergeableObject(
				new ByteArrayId(
						key.getColumnFamily().getBytes()),
				new ByteArrayId(
						key.getColumnQualifier().getBytes()),
				binary);
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
		rowTransform = (RowTransform<Mergeable>) AccumuloUtils.fromBinary(rowTransformBytes);
		rowTransform.initOptions(options);
	}

}
