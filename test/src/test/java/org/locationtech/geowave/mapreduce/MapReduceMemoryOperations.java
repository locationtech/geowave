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
package org.locationtech.geowave.mapreduce;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.memory.MemoryDataStoreOperations;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;

import com.beust.jcommander.internal.Lists;

public class MapReduceMemoryOperations extends
		MemoryDataStoreOperations implements
		MapReduceDataStoreOperations
{

	private final Map<ByteArray, SortedSet<MemoryStoreEntry>> storeData = Collections
			.synchronizedMap(new HashMap<ByteArray, SortedSet<MemoryStoreEntry>>());

	@Override
	public <T> RowReader<T> createReader(
			RecordReaderParams<T> readerParams ) {

		ByteArray partitionKey = new ByteArray(
				readerParams.getRowRange().getPartitionKey() == null ? new byte[0] : readerParams
						.getRowRange()
						.getPartitionKey());

		ByteArrayRange sortRange = new ByteArrayRange(
				new ByteArray(
						readerParams.getRowRange().getStartSortKey() == null ? new byte[0] : readerParams
								.getRowRange()
								.getStartSortKey()),
				new ByteArray(
						readerParams.getRowRange().getEndSortKey() == null ? new byte[0] : readerParams
								.getRowRange()
								.getEndSortKey()));

		return createReader((ReaderParams) new ReaderParams(
				readerParams.getIndex(),
				readerParams.getAdapterStore(),
				readerParams.getInternalAdapterStore(),
				readerParams.getAdapterIds(),
				readerParams.getMaxResolutionSubsamplingPerDimension(),
				readerParams.getAggregation(),
				readerParams.getFieldSubsets(),
				readerParams.isMixedVisibility(),
				readerParams.isServersideAggregation(),
				false,
				false,
				new QueryRanges(
						Collections.singleton(new SinglePartitionQueryRanges(
								partitionKey,
								Collections.singleton(sortRange)))),
				readerParams.getFilter(),
				readerParams.getLimit(),
				readerParams.getMaxRangeDecomposition(),
				readerParams.getCoordinateRanges(),
				readerParams.getConstraints(),
				readerParams.getRowTransformer(),
				readerParams.getAdditionalAuthorizations()));

	}

}
