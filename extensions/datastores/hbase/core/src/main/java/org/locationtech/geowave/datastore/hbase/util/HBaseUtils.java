/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.util;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.server.ServerOpConfig.ServerOpScope;
import org.locationtech.geowave.mapreduce.URLClassloaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class HBaseUtils {
  private static final CellComparator CELL_COMPARATOR = CellComparator.getInstance();

  public static CellComparator getCellComparator() {
    return CELL_COMPARATOR;
  }

  public static String getQualifiedTableName(
      final String tableNamespace,
      final String unqualifiedTableName) {
    if ((tableNamespace == null) || tableNamespace.isEmpty()) {
      return unqualifiedTableName;
    }

    if (unqualifiedTableName.contains(tableNamespace)) {
      return unqualifiedTableName;
    }

    return tableNamespace + "_" + unqualifiedTableName;
  }

  public static String writeTableNameAsConfigSafe(final String tableName) {
    // '.' is a special separator character used by the coprocessor config,
    // and ':' should be safe to use in the coprocessor config because it is
    // a special HBase table character that cannot be used in a
    // table namespace or qualifier (its meant to separate the table
    // namespace and the qualifier)
    return tableName.replaceAll("\\.", ":");
  }

  public static String readConfigSafeTableName(final String safeTableName) {
    // just reverse the replacement to ':' to return the table name to the
    // original
    return safeTableName.replaceAll(":", "\\.");
  }

  public static QueryRanges constraintsToByteArrayRanges(
      final MultiDimensionalNumericData constraints,
      final NumericIndexStrategy indexStrategy,
      final int maxRanges) {
    if ((constraints == null) || constraints.isEmpty()) {
      return null; // implies in negative and
      // positive infinity
    } else {
      return indexStrategy.getQueryRanges(constraints, maxRanges);
    }
  }

  public static RowMutations getDeleteMutations(
      final byte[] rowId,
      final byte[] columnFamily,
      final byte[] columnQualifier,
      final String[] authorizations) throws IOException {
    final RowMutations m = new RowMutations(rowId);
    final Delete d = new Delete(rowId);
    d.addColumns(columnFamily, columnQualifier);
    m.add(d);
    return m;
  }

  public static class ScannerClosableWrapper implements Closeable {
    private final ResultScanner results;

    public ScannerClosableWrapper(final ResultScanner results) {
      this.results = results;
    }

    @Override
    public void close() {
      results.close();
    }
  }

  public static class MultiScannerClosableWrapper implements Closeable {
    private final List<ResultScanner> results;

    public MultiScannerClosableWrapper(final List<ResultScanner> results) {
      this.results = results;
    }

    @Override
    public void close() {
      for (final ResultScanner scanner : results) {
        scanner.close();
      }
    }
  }

  public static StatisticValue<?> getMergedStats(final List<Cell> rowCells) {
    StatisticValue<?> mergedStats = null;
    for (final Cell cell : rowCells) {
      final byte[] byteValue = CellUtil.cloneValue(cell);
      final StatisticValue<?> stats = (StatisticValue<?>) URLClassloaderUtils.fromBinary(byteValue);

      if (mergedStats != null) {
        mergedStats.merge(stats);
      } else {
        mergedStats = stats;
      }
    }

    return mergedStats;
  }

  public static ImmutableSet<ServerOpScope> stringToScopes(final String value) {
    final String[] scopes = value.split(",");
    return Sets.immutableEnumSet(
        Iterables.transform(Arrays.asList(scopes), new Function<String, ServerOpScope>() {

          @Override
          public ServerOpScope apply(final String input) {
            return ServerOpScope.valueOf(input);
          }
        }));
  }

  /**
   * Since HBase's end keys are always exclusive, just add a trailing zero if you want an inclusive
   * row range.
   */
  public static byte[] getInclusiveEndKey(final byte[] endKey) {
    final byte[] inclusiveEndKey = new byte[endKey.length + 1];

    System.arraycopy(endKey, 0, inclusiveEndKey, 0, inclusiveEndKey.length - 1);

    return inclusiveEndKey;
  }
}
