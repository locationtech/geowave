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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class ConnectionPool
{
	private static ConnectionPool singletonInstance;

	public static synchronized ConnectionPool getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new ConnectionPool();
		}
		return singletonInstance;
	}

	private final Map<String, Connection> connectorCache = new HashMap<String, Connection>();
	private static final String HBASE_CONFIGURATION_TIMEOUT = "timeout";
	private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";

	public synchronized Connection getConnection(
			final String zookeeperInstances )
			throws IOException {
		Connection connection = connectorCache.get(zookeeperInstances);
		if (connection == null) {
			final Configuration hConf = HBaseConfiguration.create();
			hConf.set(
					HBASE_CONFIGURATION_ZOOKEEPER_QUORUM,
					zookeeperInstances);
			hConf.setInt(
					HBASE_CONFIGURATION_TIMEOUT,
					120000);
			connection = ConnectionFactory.createConnection(hConf);
			connectorCache.put(
					zookeeperInstances,
					connection);
		}
		return connection;
	}
}
