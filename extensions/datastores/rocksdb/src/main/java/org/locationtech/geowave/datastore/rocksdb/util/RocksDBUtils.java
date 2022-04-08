/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.RangeReaderParams;
import com.google.common.collect.Streams;
import com.google.common.primitives.UnsignedBytes;

public class RocksDBUtils {
  protected static final int MAX_ROWS_FOR_PAGINATION = 1000000;
  public static int ROCKSDB_DEFAULT_MAX_RANGE_DECOMPOSITION = 250;
  public static int ROCKSDB_DEFAULT_AGGREGATION_MAX_RANGE_DECOMPOSITION = 250;
  public static ByteArray EMPTY_PARTITION_KEY = new ByteArray();

  public static RocksDBMetadataTable getMetadataTable(
      final RocksDBClient client,
      final MetadataType metadataType) {
    // stats also store a timestamp because stats can be the exact same but
    // need to still be unique (consider multiple count statistics that are
    // exactly the same count, but need to be merged)
    return client.getMetadataTable(metadataType);
  }

  public static String getTablePrefix(final String typeName, final String indexName) {
    return typeName + "_" + indexName;
  }

  public static RocksDBDataIndexTable getDataIndexTable(
      final RocksDBClient client,
      final String typeName,
      final short adapterId) {
    return client.getDataIndexTable(
        getTablePrefix(typeName, DataIndexUtils.DATA_ID_INDEX.getName()),
        adapterId);
  }

  public static RocksDBIndexTable getIndexTableFromPrefix(
      final RocksDBClient client,
      final String namePrefix,
      final short adapterId,
      final byte[] partitionKey,
      final boolean requiresTimestamp) {
    return getIndexTable(
        client,
        getTableName(namePrefix, partitionKey),
        adapterId,
        partitionKey,
        requiresTimestamp);
  }

  public static String getTableName(
      final String typeName,
      final String indexName,
      final byte[] partitionKey) {
    return getTableName(getTablePrefix(typeName, indexName), partitionKey);
  }

  public static String getTableName(final String setNamePrefix, final byte[] partitionKey) {
    String partitionStr;
    if ((partitionKey != null) && (partitionKey.length > 0)) {
      partitionStr = "_" + ByteArrayUtils.byteArrayToString(partitionKey);
    } else {
      partitionStr = "";
    }
    return setNamePrefix + partitionStr;
  }

  public static RocksDBIndexTable getIndexTable(
      final RocksDBClient client,
      final String tableName,
      final short adapterId,
      final byte[] partitionKey,
      final boolean requiresTimestamp) {
    return client.getIndexTable(tableName, adapterId, partitionKey, requiresTimestamp);
  }

  public static RocksDBIndexTable getIndexTable(
      final RocksDBClient client,
      final String typeName,
      final String indexName,
      final short adapterId,
      final byte[] partitionKey,
      final boolean requiresTimestamp) {
    return getIndexTable(
        client,
        getTablePrefix(typeName, indexName),
        adapterId,
        partitionKey,
        requiresTimestamp);
  }

  public static Set<ByteArray> getPartitions(final String directory, final String tableNamePrefix) {
    return Arrays.stream(
        new File(directory).list((dir, name) -> name.startsWith(tableNamePrefix))).map(
            str -> str.length() > (tableNamePrefix.length() + 1)
                ? new ByteArray(
                    ByteArrayUtils.byteArrayFromString(str.substring(tableNamePrefix.length() + 1)))
                : new ByteArray()).collect(Collectors.toSet());
  }

  public static boolean isSortByTime(final InternalDataAdapter<?> adapter) {
    return adapter.getAdapter() instanceof RowMergingDataAdapter;
  }

  public static boolean isSortByKeyRequired(final RangeReaderParams<?> params) {
    // subsampling needs to be sorted by sort key to work properly
    return (params.getMaxResolutionSubsamplingPerDimension() != null)
        && (params.getMaxResolutionSubsamplingPerDimension().length > 0);
  }

  public static Iterator<GeoWaveRow> sortBySortKey(final Iterator<GeoWaveRow> it) {
    return Streams.stream(it).sorted(SortKeyOrder.SINGLETON).iterator();
  }

  public static Pair<Boolean, Boolean> isGroupByRowAndIsSortByTime(
      final RangeReaderParams<?> readerParams,
      final short adapterId) {
    final boolean sortByTime = isSortByTime(readerParams.getAdapterStore().getAdapter(adapterId));
    return Pair.of(readerParams.isMixedVisibility() || sortByTime, sortByTime);
  }

  private static class SortKeyOrder implements Comparator<GeoWaveRow>, Serializable {
    private static SortKeyOrder SINGLETON = new SortKeyOrder();
    private static final long serialVersionUID = 23275155231L;

    @Override
    public int compare(final GeoWaveRow o1, final GeoWaveRow o2) {
      if (o1 == o2) {
        return 0;
      }
      if (o1 == null) {
        return 1;
      }
      if (o2 == null) {
        return -1;
      }
      byte[] otherComp = o2.getSortKey() == null ? new byte[0] : o2.getSortKey();
      byte[] thisComp = o1.getSortKey() == null ? new byte[0] : o1.getSortKey();

      int comp = UnsignedBytes.lexicographicalComparator().compare(thisComp, otherComp);
      if (comp != 0) {
        return comp;
      }
      otherComp = o2.getPartitionKey() == null ? new byte[0] : o2.getPartitionKey();
      thisComp = o1.getPartitionKey() == null ? new byte[0] : o1.getPartitionKey();

      comp = UnsignedBytes.lexicographicalComparator().compare(thisComp, otherComp);
      if (comp != 0) {
        return comp;
      }
      comp = Short.compare(o1.getAdapterId(), o2.getAdapterId());
      if (comp != 0) {
        return comp;
      }
      otherComp = o2.getDataId() == null ? new byte[0] : o2.getDataId();
      thisComp = o1.getDataId() == null ? new byte[0] : o1.getDataId();

      comp = UnsignedBytes.lexicographicalComparator().compare(thisComp, otherComp);

      if (comp != 0) {
        return comp;
      }
      return Integer.compare(o1.getNumberOfDuplicates(), o2.getNumberOfDuplicates());
    }
  }
}
