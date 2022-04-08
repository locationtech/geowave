/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.kafka;

import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.locationtech.geowave.test.TestEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTestEnvironment implements TestEnvironment {

  private static KafkaTestEnvironment singletonInstance;

  public static synchronized KafkaTestEnvironment getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new KafkaTestEnvironment();
    }
    return singletonInstance;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestEnvironment.class);

  private EmbeddedKafkaCluster kafkaServer;

  private String bootstrapServers;

  private KafkaTestEnvironment() {}

  @Override
  public void setup() throws Exception {
    if (kafkaServer == null) {
      LOGGER.info("Starting up Kafka Server...");

      FileUtils.deleteDirectory(KafkaTestUtils.DEFAULT_LOG_DIR);

      final boolean success = KafkaTestUtils.DEFAULT_LOG_DIR.mkdir();
      if (!success) {
        LOGGER.warn(
            "Unable to create Kafka log dir ["
                + KafkaTestUtils.DEFAULT_LOG_DIR.getAbsolutePath()
                + "]");
      }

      String localhost = "localhost";
      try {
        localhost = java.net.InetAddress.getLocalHost().getCanonicalHostName();
      } catch (final UnknownHostException e) {
        LOGGER.warn("unable to get canonical hostname for localhost", e);
      }

      final Properties config = KafkaTestUtils.getKafkaBrokerConfig(localhost);
      kafkaServer = new EmbeddedKafkaCluster(1, config);
      bootstrapServers = localhost + ":9092";
      kafkaServer.start();
    }
  }

  @Override
  public void tearDown() throws Exception {
    LOGGER.info("Shutting down Kafka Server...");
    if (kafkaServer != null) {
      final Method m = kafkaServer.getClass().getDeclaredMethod("after");
      m.setAccessible(true);
      m.invoke(kafkaServer);
      kafkaServer = null;
    }
    FileUtils.forceDeleteOnExit(KafkaTestUtils.DEFAULT_LOG_DIR);
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  @Override
  public TestEnvironment[] getDependentEnvironments() {
    return new TestEnvironment[] {};
  }
}
