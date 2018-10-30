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
package org.locationtech.geowave.datastore.hbase.server;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;
import org.locationtech.geowave.core.store.server.RowMergingAdapterOptionProvider;
import org.locationtech.geowave.mapreduce.URLClassloaderUtils;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class RowMergingServerOp extends
		MergingServerOp
{
	private RowTransform<Mergeable> rowTransform;

	@Override
	protected Mergeable getMergeable(
			final Cell cell,
			final byte[] bytes ) {
		return rowTransform.getRowAsMergeableObject(
				ByteArrayUtils.shortFromString(StringUtils.stringFromBinary(CellUtil.cloneFamily(cell))),
				new ByteArray(
						CellUtil.cloneQualifier(cell)),
				bytes);
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
			final Map<String, String> options )
			throws IOException {
		final String columnStr = options.get(RowMergingAdapterOptionProvider.ADAPTER_IDS_OPTION);

		if (columnStr.length() == 0) {
			throw new IllegalArgumentException(
					"The column must not be empty");
		}

		columnFamilyIds = Sets.newHashSet(Iterables.transform(
				Splitter.on(
						",").split(
						columnStr),
				new Function<String, GeowaveColumnId>() {

					@Override
					public GeowaveColumnId apply(
							final String input ) {
						return new ShortColumnId(
								ByteArrayUtils.shortFromString(input));
					}
				}));

		final String rowTransformStr = options.get(RowMergingAdapterOptionProvider.ROW_TRANSFORM_KEY);
		final byte[] rowTransformBytes = ByteArrayUtils.byteArrayFromString(rowTransformStr);
		rowTransform = (RowTransform<Mergeable>) URLClassloaderUtils.fromBinary(rowTransformBytes);
		rowTransform.initOptions(options);
	}

}
