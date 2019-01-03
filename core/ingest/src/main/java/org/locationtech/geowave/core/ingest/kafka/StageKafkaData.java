/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.avro.specific.SpecificRecordBase;
import org.locationtech.geowave.core.ingest.avro.GeoWaveAvroFormatPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to hold intermediate stage data that must be used throughout the life of the Kafka stage
 * process.
 */
public class StageKafkaData<T extends SpecificRecordBase> {

  private static final Logger LOGGER = LoggerFactory.getLogger(StageKafkaData.class);
  private final Map<String, Producer<String, T>> cachedProducers =
      new HashMap<String, Producer<String, T>>();
  private final Properties properties;

  public StageKafkaData(final Properties properties) {
    this.properties = properties;
  }

  public Producer<String, T> getProducer(
      final String typeName,
      final GeoWaveAvroFormatPlugin<?, ?> plugin) {
    return getProducerCreateIfNull(typeName, plugin);
  }

  private synchronized Producer<String, T> getProducerCreateIfNull(
      final String typeName,
      final GeoWaveAvroFormatPlugin<?, ?> plugin) {
    if (!cachedProducers.containsKey(typeName)) {
      final ProducerConfig producerConfig = new ProducerConfig(properties);

      final Producer<String, T> producer = new Producer<String, T>(producerConfig);

      cachedProducers.put(typeName, producer);
    }
    return cachedProducers.get(typeName);
  }

  public synchronized void close() {
    for (final Producer<String, T> producer : cachedProducers.values()) {
      try {
        producer.close();
      } catch (final Exception e) {
        LOGGER.warn("Unable to close kafka producer", e);
      }
    }
    cachedProducers.clear();
  }
}
