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
package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.cli.VersionUtils;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeowavePersistence;
import mil.nga.giat.geowave.datastore.hbase.metadata.AbstractHBasePersistence;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.query.protobuf.VersionProtos;
import mil.nga.giat.geowave.datastore.hbase.query.protobuf.VersionProtos.VersionRequest;
import mil.nga.giat.geowave.datastore.hbase.query.protobuf.VersionProtos.VersionService;

public class HBaseVersionQuery
{
	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseVersionQuery.class);
	private final BasicHBaseOperations operations;

	public HBaseVersionQuery(
			final BasicHBaseOperations operations ) {
		this.operations = operations;
	}

	public String queryVersion(
			final HBaseOptions hbaseOptions ) {
		String version = null;

		if ((hbaseOptions == null) || !hbaseOptions.isEnableCoprocessors()) {
			LOGGER.warn("Coprocessors not enabled, serverside version is irrelevant");
			return null;
		}
		try {
			if (!operations.tableExists(AbstractGeowavePersistence.METADATA_TABLE)) {
				operations.createTable(
						AbstractHBasePersistence.METADATA_CFS,
						BasicHBaseOperations.getTableName(operations
								.getQualifiedTableName(AbstractGeowavePersistence.METADATA_TABLE)),
						null);
			}

			// Use the row count coprocessor
			if (hbaseOptions.isVerifyCoprocessors()) {
				operations.verifyCoprocessor(
						AbstractGeowavePersistence.METADATA_TABLE,
						VersionService.class.getName(),
						hbaseOptions.getCoprocessorJar());
			}
			final Table table = operations.getTable(AbstractGeowavePersistence.METADATA_TABLE);
			final Map<byte[], List<String>> versionInfoResponse = table.coprocessorService(
					VersionProtos.VersionService.class,
					null,
					null,
					new Batch.Call<VersionProtos.VersionService, List<String>>() {
						@Override
						public List<String> call(
								final VersionProtos.VersionService versionService )
								throws IOException {
							final BlockingRpcCallback<VersionProtos.VersionResponse> rpcCallback = new BlockingRpcCallback<VersionProtos.VersionResponse>();
							versionService.version(
									null,
									VersionRequest.getDefaultInstance(),
									rpcCallback);
							final VersionProtos.VersionResponse response = rpcCallback.get();
							return response.getVersionInfoList();
						}
					});
			if ((versionInfoResponse == null) || versionInfoResponse.isEmpty()) {
				LOGGER.error("No response from version coprocessor");
			}
			else {
				final Iterator<List<String>> values = versionInfoResponse.values().iterator();

				final List<String> value = values.next();
				while (values.hasNext()) {
					final List<String> newValue = values.next();
					if (!value.equals(newValue)) {
						LOGGER
								.error("Version Info '"
										+ Arrays.toString(value.toArray())
										+ "' and '"
										+ Arrays.toString(newValue.toArray())
										+ "' differ.  This may mean that different regions are using different versions of GeoWave.");
					}
				}
				version = VersionUtils.asLineDelimitedString(value);
			}
		}
		catch (final Throwable e) {
			LOGGER.warn(
					"Unable to check metadata table for version",
					e);
		}
		return version;
	}
}
