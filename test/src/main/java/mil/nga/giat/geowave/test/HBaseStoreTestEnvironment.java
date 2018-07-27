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
package mil.nga.giat.geowave.test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.visibility.ScanLabelGenerator;
import org.apache.hadoop.hbase.security.visibility.SimpleScanLabelGenerator;
import org.apache.hadoop.hbase.security.visibility.VisibilityClient;
import org.apache.hadoop.hbase.security.visibility.VisibilityLabelService;
import org.apache.hadoop.hbase.security.visibility.VisibilityLabelServiceManager;
import org.apache.hadoop.hbase.security.visibility.VisibilityTestUtil;
import org.apache.hadoop.hbase.security.visibility.VisibilityUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HbaseLocalCluster;
import com.github.sakserv.propertyparser.PropertyParser;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.hbase.HBaseStoreFactoryFamily;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseRequiredOptions;
import mil.nga.giat.geowave.datastore.hbase.util.ConnectionPool;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

public class HBaseStoreTestEnvironment extends
		StoreTestEnvironment
{
	private static final GenericStoreFactory<DataStore> STORE_FACTORY = new HBaseStoreFactoryFamily()
			.getDataStoreFactory();
	private static HBaseStoreTestEnvironment singletonInstance = null;

	private static boolean enableVisibility = true;

	public static synchronized HBaseStoreTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new HBaseStoreTestEnvironment();
		}
		return singletonInstance;
	}

	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseStoreTestEnvironment.class);
	public static final String HBASE_PROPS_FILE = "hbase.properties";
	protected String zookeeper;

	private HbaseLocalCluster hbaseLocalCluster;

	private HBaseStoreTestEnvironment() {}

	// VisibilityTest valid authorizations
	private static String[] auths = new String[] {
		"a",
		"b",
		"c",
		"z"
	};

	protected User SUPERUSER;

	@Override
	protected void initOptions(
			final StoreFactoryOptions options ) {
		HBaseRequiredOptions hbaseRequiredOptions = (HBaseRequiredOptions) options;
		hbaseRequiredOptions.setZookeeper(zookeeper);
	}

	@Override
	protected GenericStoreFactory<DataStore> getDataStoreFactory() {
		return STORE_FACTORY;
	}

	@Override
	public void setup() {
		if (hbaseLocalCluster == null) {
			PropertyParser propertyParser = null;

			try {
				propertyParser = new PropertyParser(
						HBASE_PROPS_FILE);
				propertyParser.parsePropsFile();
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to load property file: {}" + HBASE_PROPS_FILE,
						e);
			}

			if (!TestUtils.isSet(zookeeper)) {
				zookeeper = System.getProperty(ZookeeperTestEnvironment.ZK_PROPERTY_NAME);

				if (!TestUtils.isSet(zookeeper)) {
					zookeeper = ZookeeperTestEnvironment.getInstance().getZookeeper();
					LOGGER.debug("Using local zookeeper URL: " + zookeeper);
				}
			}

			if (!TestUtils.isSet(System.getProperty(ZookeeperTestEnvironment.ZK_PROPERTY_NAME))) {
				try {
					final Configuration conf = new Configuration();
					conf.set(
							"hbase.online.schema.update.enable",
							"true");

					if (enableVisibility) {
						conf.set(
								"hbase.superuser",
								"admin");

						conf.setBoolean(
								"hbase.security.authorization",
								true);

						conf.setBoolean(
								"hbase.security.visibility.mutations.checkauths",
								true);

						// setup vis IT configuration
						conf.setClass(
								VisibilityUtils.VISIBILITY_LABEL_GENERATOR_CLASS,
								SimpleScanLabelGenerator.class,
								ScanLabelGenerator.class);

						conf.setClass(
								VisibilityLabelServiceManager.VISIBILITY_LABEL_SERVICE_CLASS,
								// DefaultVisibilityLabelServiceImpl.class,
								HBaseTestVisibilityLabelServiceImpl.class,
								VisibilityLabelService.class);

						// Install the VisibilityController as a system
						// processor
						VisibilityTestUtil.enableVisiblityLabels(conf);
					}

					// Start the cluster
					hbaseLocalCluster = new HbaseLocalCluster.Builder()
							.setHbaseMasterPort(
									Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_PORT_KEY)))
							.setHbaseMasterInfoPort(
									Integer.parseInt(propertyParser.getProperty(ConfigVars.HBASE_MASTER_INFO_PORT_KEY)))
							.setNumRegionServers(
									Integer.parseInt(propertyParser
											.getProperty(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY)))
							.setHbaseRootDir(
									propertyParser.getProperty(ConfigVars.HBASE_ROOT_DIR_KEY))
							.setZookeeperPort(
									Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
							.setZookeeperConnectionString(
									propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
							.setZookeeperZnodeParent(
									propertyParser.getProperty(ConfigVars.HBASE_ZNODE_PARENT_KEY))
							.setHbaseWalReplicationEnabled(
									Boolean.parseBoolean(propertyParser
											.getProperty(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY)))
							.setHbaseConfiguration(
									conf)
							.build();
					hbaseLocalCluster.start();

					if (enableVisibility) {

						// Set valid visibilities for the vis IT
						final Connection conn = ConnectionPool.getInstance().getConnection(
								zookeeper);
						try {
							SUPERUSER = User.createUserForTesting(
									conf,
									"admin",
									new String[] {
										"supergroup"
									});

							// Set up valid visibilities for the user
							addLabels(
									conn.getConfiguration(),
									auths,
									User.getCurrent().getName());

							// Verify hfile version
							final String hfileVersionStr = conn.getAdmin().getConfiguration().get(
									"hfile.format.version");
							Assert.assertTrue(
									"HFile version is incorrect",
									hfileVersionStr.equals("3"));
						}
						catch (final Throwable e) {
							LOGGER.error(
									"Error creating test user",
									e);
						}
					}
				}
				catch (final Exception e) {
					LOGGER.error(
							"Exception starting hbaseLocalCluster",
							e);
					Assert.fail();
				}
			}
		}
	}

	private void addLabels(
			final Configuration conf,
			final String[] labels,
			final String user )
			throws Exception {
		final PrivilegedExceptionAction<VisibilityLabelsResponse> action = new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
			@Override
			public VisibilityLabelsResponse run()
					throws Exception {
				try {
					VisibilityClient.addLabels(
							conf,
							labels);

					VisibilityClient.setAuths(
							conf,
							labels,
							user);
				}
				catch (final Throwable t) {
					throw new IOException(
							t);
				}
				return null;
			}
		};

		SUPERUSER.runAs(action);
	}

	@Override
	public void tearDown() {
		try {
			hbaseLocalCluster.stop(true);
			hbaseLocalCluster = null;
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to delete mini hbase temporary directory",
					e);
		}
	}

	@Override
	protected GeoWaveStoreType getStoreType() {
		return GeoWaveStoreType.HBASE;
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {
			ZookeeperTestEnvironment.getInstance()
		};
	}
}
