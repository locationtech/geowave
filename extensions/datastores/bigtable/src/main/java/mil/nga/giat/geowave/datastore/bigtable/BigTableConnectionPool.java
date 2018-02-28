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
package mil.nga.giat.geowave.datastore.bigtable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;

public class BigTableConnectionPool
{
	private static BigTableConnectionPool singletonInstance;

	public static synchronized BigTableConnectionPool getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new BigTableConnectionPool();
		}
		return singletonInstance;
	}

	private final Map<String, Connection> connectorCache = new HashMap<String, Connection>();
	private static final String HBASE_CONFIGURATION_TIMEOUT = "timeout";

	public synchronized Connection getConnection(
			final String projectId,
			final String instanceId )
			throws IOException {
		String key = projectId + "_" + instanceId;
		Connection connection = connectorCache.get(key);
		if (connection == null) {
			final Configuration config = BigtableConfiguration.configure(
					projectId,
					instanceId);

			config.setInt(
					HBASE_CONFIGURATION_TIMEOUT,
					120000);

			connection = BigtableConfiguration.connect(config);
			connectorCache.put(
					key,
					connection);
		}

		return connection;
	}
}
