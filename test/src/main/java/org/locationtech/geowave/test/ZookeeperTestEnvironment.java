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
package org.locationtech.geowave.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;

public class ZookeeperTestEnvironment implements
		TestEnvironment
{

	private static ZookeeperTestEnvironment singletonInstance = null;

	public static synchronized ZookeeperTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new ZookeeperTestEnvironment();
		}
		return singletonInstance;
	}

	private final static Logger LOGGER = LoggerFactory.getLogger(ZookeeperTestEnvironment.class);
	protected String zookeeper;

	private Object zookeeperLocalCluster;

	public static final String ZK_PROPERTY_NAME = "zookeeperUrl";
	public static final String DEFAULT_ZK_TEMP_DIR = "./target/zk_temp";

	private ZookeeperTestEnvironment() {}

	@Override
	public void setup()
			throws Exception {
		if (!TestUtils.isSet(zookeeper)) {
			zookeeper = System.getProperty(ZK_PROPERTY_NAME);

			if (!TestUtils.isSet(zookeeper)) {

				try {
					ClassLoader prevCl = Thread.currentThread().getContextClassLoader();
					ClassLoader hbaseMiniClusterCl = HBaseMiniClusterClassLoader.getInstance(prevCl);
					Thread.currentThread().setContextClassLoader(
							hbaseMiniClusterCl);
					Configuration conf = (Configuration) Class.forName(
							"org.apache.hadoop.hbase.HBaseConfiguration",
							true,
							hbaseMiniClusterCl).getMethod(
							"create").invoke(
							null);
					conf.setInt(
							"test.hbase.zookeeper.property.clientPort",
							2181);
					System.setProperty(
							"test.build.data.basedirectory",
							conf.get(
									"zookeeper.temp.dir",
									DEFAULT_ZK_TEMP_DIR));
					zookeeperLocalCluster = Class.forName(
							"org.apache.hadoop.hbase.HBaseTestingUtility",
							true,
							hbaseMiniClusterCl).getConstructor(
							Configuration.class).newInstance(
							conf);
					zookeeperLocalCluster.getClass().getMethod(
							"startMiniZKCluster").invoke(
							zookeeperLocalCluster);
					Thread.currentThread().setContextClassLoader(
							prevCl);
				}
				catch (final Exception e) {
					LOGGER.error(
							"Exception starting zookeeperLocalCluster: " + e,
							e);
					Assert.fail();
				}
				Object zkCluster = zookeeperLocalCluster.getClass().getMethod(
						"getZkCluster").invoke(
						zookeeperLocalCluster);
				zookeeper = "127.0.0.1:" + zkCluster.getClass().getMethod(
						"getClientPort").invoke(
						zkCluster);
			}
		}
	}

	@Override
	public void tearDown()
			throws Exception {
		try {
			zookeeperLocalCluster.getClass().getMethod(
					"shutdownMiniZKCluster").invoke(
					zookeeperLocalCluster);
			if (!(Boolean) zookeeperLocalCluster.getClass().getMethod(
					"cleanupTestDir").invoke(
					zookeeperLocalCluster)) {
				LOGGER.warn("Unable to delete mini zookeeper temporary directory");
			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to delete or shutdown mini zookeeper temporary directory",
					e);
		}

		zookeeper = null;
	}

	public String getZookeeper() {
		return zookeeper;
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {};
	}

}
