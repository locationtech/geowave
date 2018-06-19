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
package mil.nga.giat.geowave.datastore.bigtable.operations;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import com.google.cloud.bigtable.hbase.BigtableRegionLocator;
import com.google.common.collect.Sets;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.datastore.bigtable.BigTableConnectionPool;
import mil.nga.giat.geowave.datastore.bigtable.operations.config.BigTableOptions;
import mil.nga.giat.geowave.datastore.hbase.operations.HBaseOperations;

public class BigTableOperations extends
		HBaseOperations
{
	private final HashSet<String> tableCache = Sets.newHashSet();

	public BigTableOperations(
			final BigTableOptions options )
			throws IOException {
		super(
				BigTableConnectionPool.getInstance().getConnection(
						options.getProjectId(),
						options.getInstanceId()),
				options.getGeowaveNamespace(),
				options.getHBaseOptions());
	}

	@Override
	public RegionLocator getRegionLocator(
			final String tableName )
			throws IOException {
		final BigtableRegionLocator regionLocator = (BigtableRegionLocator) super.getRegionLocator(tableName);

		if (regionLocator != null) {
			// Force region update
			if (regionLocator.getAllRegionLocations().size() <= 1) {
				regionLocator.getRegionLocation(
						HConstants.EMPTY_BYTE_ARRAY,
						true);
			}
		}

		return regionLocator;
	}

	@Override
	public boolean parallelDecodeEnabled() {
		// TODO: Rows that should be merged are ending up in different regions
		// which causes parallel decode to return incorrect results.
		return false;
	}

	protected void forceRegionUpdate(
			final BigtableRegionLocator regionLocator ) {

	}

	@Override
	public Iterable<Result> getScannedResults(
			Scan scanner,
			String tableName )
			throws IOException {

		// Check the local cache
		boolean tableAvailable = tableCache.contains(tableName);

		// No local cache. Check the server and update cache
		if (!tableAvailable) {
			if (indexExists(new ByteArrayId(
					tableName))) {
				tableAvailable = true;

				tableCache.add(tableName);
			}
		}

		// Get the results if available
		if (tableAvailable) {
			return super.getScannedResults(
					scanner,
					tableName);
		}

		// Otherwise, return empty results
		return Collections.emptyList();
	}

	public static BigTableOperations createOperations(
			final BigTableOptions options )
			throws IOException {
		return new BigTableOperations(
				options);
	}

}
