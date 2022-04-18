/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.kafka;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.ingest.operations.KafkaToGeoWaveCommand;
import org.locationtech.geowave.core.ingest.operations.LocalToKafkaCommand;
import org.locationtech.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.cli.store.AddStoreCommand;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexPluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTestUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestEnvironment.class);
  private static final String MAX_MESSAGE_BYTES = "5000000";

  protected static final File DEFAULT_LOG_DIR = new File(TestUtils.TEMP_DIR, "kafka-logs");

  public static void testKafkaStage(final String ingestFilePath) throws Exception {
    // Ingest Formats
    final IngestFormatPluginOptions ingestFormatOptions = new IngestFormatPluginOptions();
    ingestFormatOptions.selectPlugin("gpx");

    final LocalToKafkaCommand localToKafka = new LocalToKafkaCommand();
    localToKafka.setParameters(ingestFilePath);
    localToKafka.setPluginFormats(ingestFormatOptions);
    localToKafka.getKafkaOptions().setBootstrapServers(
        KafkaTestEnvironment.getInstance().getBootstrapServers());
    localToKafka.getKafkaOptions().setRetryBackoffMs("1000");
    localToKafka.execute(new ManualOperationParams());
  }

  public static void testKafkaIngest(
      final DataStorePluginOptions options,
      final boolean spatialTemporal,
      final String ingestFilePath) throws Exception {
    LOGGER.warn("Ingesting '" + ingestFilePath + "' - this may take several minutes...");

    // // FIXME
    // final String[] args = StringUtils.split("-kafkaingest" +
    // " -f gpx -batchSize 1 -consumerTimeoutMs 5000 -reconnectOnTimeout
    // -groupId testGroup"
    // + " -autoOffsetReset smallest -fetchMessageMaxBytes " +
    // MAX_MESSAGE_BYTES +
    // " -zookeeperConnect " + zookeeper + " -" +

    // Ingest Formats
    final IngestFormatPluginOptions ingestFormatOptions = new IngestFormatPluginOptions();
    ingestFormatOptions.selectPlugin("gpx");

    // Indexes
    final IndexPluginOptions indexOption = new IndexPluginOptions();
    indexOption.selectPlugin((spatialTemporal ? "spatial_temporal" : "spatial"));

    // Execute Command
    final KafkaToGeoWaveCommand kafkaToGeowave = new KafkaToGeoWaveCommand();
    final File configFile = File.createTempFile("test_stats", null);
    final ManualOperationParams params = new ManualOperationParams();

    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);
    final AddStoreCommand addStore = new AddStoreCommand();
    addStore.setParameters("test-store");
    addStore.setPluginOptions(options);
    addStore.execute(params);

    final IndexStore indexStore = options.createIndexStore();
    final DataStore dataStore = options.createDataStore();
    if (indexStore.getIndex("testIndex") == null) {
      indexOption.setName("testIndex");
      dataStore.addIndex(indexOption.createIndex(dataStore));
    }

    kafkaToGeowave.setPluginFormats(ingestFormatOptions);
    kafkaToGeowave.getKafkaOptions().setBootstrapServers(
        KafkaTestEnvironment.getInstance().getBootstrapServers());
    kafkaToGeowave.getKafkaOptions().setConsumerTimeoutMs("5000");
    kafkaToGeowave.getKafkaOptions().setReconnectOnTimeout(false);
    kafkaToGeowave.getKafkaOptions().setGroupId("testGroup");
    kafkaToGeowave.getKafkaOptions().setAutoOffsetReset("earliest");
    kafkaToGeowave.getKafkaOptions().setMaxPartitionFetchBytes(MAX_MESSAGE_BYTES);
    kafkaToGeowave.setParameters("test-store", "testIndex");

    kafkaToGeowave.execute(params);

    // Wait for ingest to complete. This works because we have set
    // Kafka Consumer to Timeout and set the timeout at 5000 ms, and
    // then not to re-connect. Since this is a unit test that should
    // be fine. Basically read all data that's in the stream and
    // finish.
    try {
      kafkaToGeowave.getDriver().waitFutures();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public static Properties getKafkaBrokerConfig(final String host) {
    final Properties props = new Properties();
    props.put("log.dirs", DEFAULT_LOG_DIR.getAbsolutePath());
    props.put("broker.id", "0");
    props.put("listeners", "PLAINTEXT://" + host + ":9092");
    props.put("message.max.bytes", MAX_MESSAGE_BYTES);
    props.put("replica.fetch.max.bytes", MAX_MESSAGE_BYTES);
    props.put("num.partitions", "1");
    return props;
  }
}
