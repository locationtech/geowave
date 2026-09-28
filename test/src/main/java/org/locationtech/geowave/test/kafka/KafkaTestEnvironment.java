/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.kafka;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
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

  private KafkaClusterTestKit kafkaCluster;

  private String bootstrapServers;

  private KafkaTestEnvironment() {}

  @Override
  public void setup() throws Exception {
    if (kafkaCluster == null) {
      LOGGER.info("Starting up Kafka Server...");

      FileUtils.deleteDirectory(KafkaTestUtils.DEFAULT_LOG_DIR);

      // one node acting as both KRaft controller and broker
      final TestKitNodes nodes =
          new TestKitNodes.Builder().setCombined(true).setNumControllerNodes(1).setNumBrokerNodes(
              1).setBaseDirectory(KafkaTestUtils.DEFAULT_LOG_DIR.toPath()).build();
      final KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(nodes);
      KafkaTestUtils.getKafkaBrokerConfig().forEach(builder::setConfigProp);
      kafkaCluster = builder.build();
      kafkaCluster.format();
      kafkaCluster.startup();
      kafkaCluster.waitForReadyBrokers();
      bootstrapServers = kafkaCluster.bootstrapServers();
    }
  }

  @Override
  public void tearDown() throws Exception {
    LOGGER.info("Shutting down Kafka Server...");
    if (kafkaCluster != null) {
      // this also deletes the cluster's directories
      kafkaCluster.close();
      kafkaCluster = null;
      bootstrapServers = null;
    }
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  @Override
  public TestEnvironment[] getDependentEnvironments() {
    return new TestEnvironment[] {};
  }
}
