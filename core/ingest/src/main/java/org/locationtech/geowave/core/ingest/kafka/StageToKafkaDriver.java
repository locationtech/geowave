/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.kafka;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.locationtech.geowave.core.ingest.avro.GenericAvroSerializer;
import org.locationtech.geowave.core.ingest.avro.GeoWaveAvroFormatPlugin;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.ingest.AbstractLocalFileDriver;
import org.locationtech.geowave.core.store.ingest.LocalInputCommandLineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class actually executes the staging of data to a Kafka topic based on the available type
 * plugin providers that are discovered through SPI.
 */
public class StageToKafkaDriver<T extends SpecificRecordBase> extends
    AbstractLocalFileDriver<GeoWaveAvroFormatPlugin<?, ?>, StageKafkaData<?>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(StageToKafkaDriver.class);

  private final Map<String, GeoWaveAvroFormatPlugin<?, ?>> ingestPlugins;
  private final KafkaProducerCommandLineOptions kafkaOptions;

  public StageToKafkaDriver(
      final KafkaProducerCommandLineOptions kafkaOptions,
      final Map<String, GeoWaveAvroFormatPlugin<?, ?>> ingestPlugins,
      final LocalInputCommandLineOptions localOptions) {
    super(localOptions);
    this.kafkaOptions = kafkaOptions;
    this.ingestPlugins = ingestPlugins;
  }

  @Override
  protected void processFile(
      final URL file,
      final String typeName,
      final GeoWaveAvroFormatPlugin<?, ?> plugin,
      final StageKafkaData<?> runData) {

    try {
      final Producer<byte[], byte[]> producer = runData.getProducer(typeName, plugin);
      try (final CloseableIterator<?> avroRecords = plugin.toAvroObjects(file)) {
        while (avroRecords.hasNext()) {
          final Object avroRecord = avroRecords.next();
          final ProducerRecord<byte[], byte[]> data =
              new ProducerRecord<>(
                  typeName,
                  GenericAvroSerializer.serialize(avroRecord, plugin.getAvroSchema()));
          producer.send(data);
        }
      }
    } catch (final Exception e) {
      LOGGER.info(
          "Unable to send file [" + file.getPath() + "] to Kafka topic: " + e.getMessage(),
          e);
    }
  }

  public boolean runOperation(final String inputPath, final File configFile) {

    final Map<String, GeoWaveAvroFormatPlugin<?, ?>> stageToKafkaPlugins = ingestPlugins;

    try {
      final StageKafkaData<T> runData = new StageKafkaData<>(kafkaOptions.getProperties());
      processInput(inputPath, configFile, stageToKafkaPlugins, runData);
      runData.close();
      return true;
    } catch (final IOException e) {
      LOGGER.error("Unable to process input", e);
      return false;
    }
  }
}
