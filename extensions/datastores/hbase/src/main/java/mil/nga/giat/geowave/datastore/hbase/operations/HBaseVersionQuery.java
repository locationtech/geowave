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
package mil.nga.giat.geowave.datastore.hbase.operations;

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
import mil.nga.giat.geowave.core.store.metadata.AbstractGeoWavePersistence;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.query.protobuf.VersionProtos;
import mil.nga.giat.geowave.datastore.hbase.query.protobuf.VersionProtos.VersionRequest;
import mil.nga.giat.geowave.datastore.hbase.query.protobuf.VersionProtos.VersionService;

public class HBaseVersionQuery
{
	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseVersionQuery.class);
	private final HBaseOperations operations;

	public HBaseVersionQuery(
			final HBaseOperations operations ) {
		this.operations = operations;
	}

}
