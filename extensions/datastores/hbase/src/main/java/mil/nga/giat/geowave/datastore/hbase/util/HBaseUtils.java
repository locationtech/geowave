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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.server.ServerOpConfig.ServerOpScope;
import mil.nga.giat.geowave.mapreduce.URLClassloaderUtils;

@SuppressWarnings("rawtypes")
public class HBaseUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseUtils.class);

	public static String getQualifiedTableName(
			final String tableNamespace,
			final String unqualifiedTableName ) {
		if ((tableNamespace == null) || tableNamespace.isEmpty()) {
			return unqualifiedTableName;
		}

		if (unqualifiedTableName.contains(tableNamespace)) {
			return unqualifiedTableName;
		}

		return tableNamespace + "_" + unqualifiedTableName;
	}

	public static String writeTableNameAsConfigSafe(
			String tableName ) {
		// '.' is a special separator character used by the coprocessor config,
		// and ':' should be safe to use in the coprocessor config because it is
		// a special HBase table character that cannot be used in a
		// table namespace or qualifier (its meant to separate the table
		// namespace and the qualifier)
		return tableName.replaceAll(
				"\\.",
				":");
	}

	public static String readConfigSafeTableName(
			String safeTableName ) {
		// just reverse the replacement to ':' to return the table name to the
		// original
		return safeTableName.replaceAll(
				":",
				"\\.");
	}

	public static QueryRanges constraintsToByteArrayRanges(
			final MultiDimensionalNumericData constraints,
			final NumericIndexStrategy indexStrategy,
			final int maxRanges ) {
		if ((constraints == null) || constraints.isEmpty()) {
			return null; // implies in negative and
			// positive infinity
		}
		else {
			return indexStrategy.getQueryRanges(
					constraints,
					maxRanges);
		}
	}

	public static RowMutations getDeleteMutations(
			final byte[] rowId,
			final byte[] columnFamily,
			final byte[] columnQualifier,
			final String[] authorizations )
			throws IOException {
		final RowMutations m = new RowMutations(
				rowId);
		final Delete d = new Delete(
				rowId);
		d.addColumns(
				columnFamily,
				columnQualifier);
		m.add(d);
		return m;
	}

	public static class ScannerClosableWrapper implements
			Closeable
	{
		private final ResultScanner results;

		public ScannerClosableWrapper(
				final ResultScanner results ) {
			this.results = results;
		}

		@Override
		public void close() {
			results.close();
		}

	}

	public static class MultiScannerClosableWrapper implements
			Closeable
	{
		private final List<ResultScanner> results;

		public MultiScannerClosableWrapper(
				final List<ResultScanner> results ) {
			this.results = results;
		}

		@Override
		public void close() {
			for (final ResultScanner scanner : results) {
				scanner.close();
			}
		}
	}

	public static DataStatistics getMergedStats(
			final List<Cell> rowCells ) {
		DataStatistics mergedStats = null;
		for (final Cell cell : rowCells) {
			final byte[] byteValue = CellUtil.cloneValue(cell);
			final DataStatistics stats = (DataStatistics) URLClassloaderUtils.fromBinary(byteValue);

			if (mergedStats != null) {
				mergedStats.merge(stats);
			}
			else {
				mergedStats = stats;
			}
		}

		return mergedStats;
	}

	public static ImmutableSet<ServerOpScope> stringToScopes(
			final String value ) {
		final String[] scopes = value.split(",");
		return Sets.immutableEnumSet(Iterables.transform(
				Arrays.asList(scopes),
				new Function<String, ServerOpScope>() {

					@Override
					public ServerOpScope apply(
							final String input ) {
						return ServerOpScope.valueOf(input);
					}
				}));
	}

	/**
	 * Since HBase's end keys are always exclusive, just add a trailing zero if
	 * you want an inclusive row range
	 *
	 * @param endKey
	 * @return
	 */
	public static byte[] getInclusiveEndKey(
			final byte[] endKey ) {
		final byte[] inclusiveEndKey = new byte[endKey.length + 1];

		System.arraycopy(
				endKey,
				0,
				inclusiveEndKey,
				0,
				inclusiveEndKey.length - 1);

		return inclusiveEndKey;
	}
}
