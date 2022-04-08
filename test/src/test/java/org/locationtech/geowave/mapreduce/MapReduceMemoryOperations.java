/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.memory.MemoryDataStoreOperations;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;

public class MapReduceMemoryOperations extends MemoryDataStoreOperations implements
    MapReduceDataStoreOperations {

  private final Map<ByteArray, SortedSet<MemoryStoreEntry>> storeData =
      Collections.synchronizedMap(new HashMap<ByteArray, SortedSet<MemoryStoreEntry>>());

  @Override
  public RowReader<GeoWaveRow> createReader(final RecordReaderParams readerParams) {

    final byte[] partitionKey =
        readerParams.getRowRange().getPartitionKey() == null ? new byte[0]
            : readerParams.getRowRange().getPartitionKey();

    final ByteArrayRange sortRange =
        new ByteArrayRange(
            readerParams.getRowRange().getStartSortKey() == null ? new byte[0]
                : readerParams.getRowRange().getStartSortKey(),
            readerParams.getRowRange().getEndSortKey() == null ? new byte[0]
                : readerParams.getRowRange().getEndSortKey());

    return createReader(
        new ReaderParams(
            readerParams.getIndex(),
            readerParams.getAdapterStore(),
            readerParams.getAdapterIndexMappingStore(),
            readerParams.getInternalAdapterStore(),
            readerParams.getAdapterIds(),
            readerParams.getMaxResolutionSubsamplingPerDimension(),
            readerParams.getAggregation(),
            readerParams.getFieldSubsets(),
            readerParams.isMixedVisibility(),
            false,
            false,
            false,
            new QueryRanges(
                Collections.singleton(
                    new SinglePartitionQueryRanges(
                        partitionKey,
                        Collections.singleton(sortRange)))),
            null,
            readerParams.getLimit(),
            readerParams.getMaxRangeDecomposition(),
            null,
            null,
            GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER,
            readerParams.getAdditionalAuthorizations()));
  }
}
