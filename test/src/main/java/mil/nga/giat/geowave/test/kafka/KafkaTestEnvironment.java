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
package mil.nga.giat.geowave.test.kafka;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import mil.nga.giat.geowave.test.TestEnvironment;
import mil.nga.giat.geowave.test.ZookeeperTestEnvironment;

public class KafkaTestEnvironment implements
		TestEnvironment

{
	private static KafkaTestEnvironment singletonInstance;

	public static synchronized KafkaTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new KafkaTestEnvironment();
		}
		return singletonInstance;
	}

	private final static Logger LOGGER = LoggerFactory.getLogger(KafkaTestEnvironment.class);

	private KafkaServerStartable kafkaServer;

	private KafkaTestEnvironment() {}

	@Override
	public void setup()
			throws Exception {
		LOGGER.info("Starting up Kafka Server...");

		FileUtils.deleteDirectory(KafkaTestUtils.DEFAULT_LOG_DIR);

		final boolean success = KafkaTestUtils.DEFAULT_LOG_DIR.mkdir();
		if (!success) {
			LOGGER.warn("Unable to create Kafka log dir [" + KafkaTestUtils.DEFAULT_LOG_DIR.getAbsolutePath() + "]");
		}
		final KafkaConfig config = KafkaTestUtils.getKafkaBrokerConfig();
		kafkaServer = new KafkaServerStartable(
				config);

		kafkaServer.startup();
		Thread.sleep(3000);
	}

	@Override
	public void tearDown()
			throws Exception {
		LOGGER.info("Shutting down Kafka Server...");
		kafkaServer.shutdown();

		FileUtils.deleteDirectory(KafkaTestUtils.DEFAULT_LOG_DIR);
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {
			ZookeeperTestEnvironment.getInstance()
		};
	}
}
