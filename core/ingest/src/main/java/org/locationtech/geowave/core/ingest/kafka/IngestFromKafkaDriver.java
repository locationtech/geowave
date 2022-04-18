/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.kafka;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.locationtech.geowave.core.ingest.avro.GenericAvroSerializer;
import org.locationtech.geowave.core.ingest.avro.GeoWaveAvroFormatPlugin;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.ingest.IndexProvider;
import org.locationtech.geowave.core.store.ingest.IngestPluginBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class executes the ingestion of intermediate data from a Kafka topic into GeoWave. */
public class IngestFromKafkaDriver {
  private static final Logger LOGGER = LoggerFactory.getLogger(IngestFromKafkaDriver.class);

  private final DataStorePluginOptions storeOptions;
  private final List<Index> indices;
  private final Map<String, GeoWaveAvroFormatPlugin<?, ?>> ingestPlugins;
  private final KafkaConsumerCommandLineOptions kafkaOptions;
  private final VisibilityHandler visibilityHandler;
  private final List<Future<?>> futures = new ArrayList<>();

  public IngestFromKafkaDriver(
      final DataStorePluginOptions storeOptions,
      final List<Index> indices,
      final Map<String, GeoWaveAvroFormatPlugin<?, ?>> ingestPlugins,
      final KafkaConsumerCommandLineOptions kafkaOptions,
      final VisibilityHandler visibilityHandler) {
    this.storeOptions = storeOptions;
    this.indices = indices;
    this.ingestPlugins = ingestPlugins;
    this.kafkaOptions = kafkaOptions;
    this.visibilityHandler = visibilityHandler;
  }

  public boolean runOperation() {

    final DataStore dataStore = storeOptions.createDataStore();

    final List<String> queue = new ArrayList<>();
    addPluginsToQueue(ingestPlugins, queue);

    configureAndLaunchPlugins(dataStore, ingestPlugins, queue);

    int counter = 0;
    while (queue.size() > 0) {
      if (counter > 30) {
        for (final String pluginFormatName : queue) {
          LOGGER.error("Unable to start up Kafka consumer for plugin [" + pluginFormatName + "]");
        }
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (final InterruptedException e) {
        LOGGER.error("Thread interrupted", e);
      }
      counter++;
    }

    if (queue.size() == 0) {
      LOGGER.info("All format plugins are now listening on Kafka topics");
    } else {
      LOGGER.warn("Unable to setup Kafka consumers for the following format plugins:");
      for (final String formatPluginName : queue) {
        LOGGER.warn("\t[" + formatPluginName + "]");
      }
      return false;
    }
    return true;
  }

  private void addPluginsToQueue(
      final Map<String, GeoWaveAvroFormatPlugin<?, ?>> pluginProviders,
      final List<String> queue) {
    queue.addAll(pluginProviders.keySet());
  }

  private void configureAndLaunchPlugins(
      final DataStore dataStore,
      final Map<String, GeoWaveAvroFormatPlugin<?, ?>> pluginProviders,
      final List<String> queue) {
    try {
      for (final Entry<String, GeoWaveAvroFormatPlugin<?, ?>> pluginProvider : pluginProviders.entrySet()) {
        final List<DataTypeAdapter<?>> adapters = new ArrayList<>();

        GeoWaveAvroFormatPlugin<?, ?> avroFormatPlugin = null;
        try {
          avroFormatPlugin = pluginProvider.getValue();

          final IngestPluginBase<?, ?> ingestWithAvroPlugin =
              avroFormatPlugin.getIngestWithAvroPlugin();
          final DataTypeAdapter<?>[] dataAdapters = ingestWithAvroPlugin.getDataAdapters();
          adapters.addAll(Arrays.asList(dataAdapters));
          final KafkaIngestRunData runData = new KafkaIngestRunData(adapters, dataStore);

          futures.add(
              launchTopicConsumer(pluginProvider.getKey(), avroFormatPlugin, runData, queue));
        } catch (final UnsupportedOperationException e) {
          LOGGER.warn(
              "Plugin provider '"
                  + pluginProvider.getKey()
                  + "' does not support ingest from Kafka",
              e);
          continue;
        }
      }
    } catch (final Exception e) {
      LOGGER.warn("Error in accessing Kafka stream", e);
    }
  }

  private Consumer<byte[], byte[]> buildKafkaConsumer() {

    final Properties kafkaProperties = kafkaOptions.getProperties();

    final Consumer<byte[], byte[]> consumer =
        new KafkaConsumer<>(
            kafkaProperties,
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer());

    return consumer;
  }

  private Future<?> launchTopicConsumer(
      final String formatPluginName,
      final GeoWaveAvroFormatPlugin<?, ?> avroFormatPlugin,
      final KafkaIngestRunData ingestRunData,
      final List<String> queue) throws IllegalArgumentException {
    final ExecutorService executorService = Executors.newFixedThreadPool(queue.size());
    return executorService.submit(new Runnable() {

      @Override
      public void run() {
        try {
          consumeFromTopic(formatPluginName, avroFormatPlugin, ingestRunData, queue);
        } catch (final Exception e) {
          LOGGER.error("Error consuming from Kafka topic [" + formatPluginName + "]", e);
        }
      }
    });
  }

  public <T> void consumeFromTopic(
      final String formatPluginName,
      final GeoWaveAvroFormatPlugin<T, ?> avroFormatPlugin,
      final KafkaIngestRunData ingestRunData,
      final List<String> queue) {

    try (final Consumer<byte[], byte[]> consumer = buildKafkaConsumer()) {
      if (consumer == null) {
        throw new RuntimeException(
            "Kafka consumer connector is null, unable to create message streams");
      }
      LOGGER.debug(
          "Kafka consumer setup for format ["
              + formatPluginName
              + "] against topic ["
              + formatPluginName
              + "]");

      queue.remove(formatPluginName);
      consumer.subscribe(Collections.singletonList(formatPluginName));
      final String timeoutMs = kafkaOptions.getConsumerTimeoutMs();
      long millis = -1;
      if ((timeoutMs != null) && !timeoutMs.trim().isEmpty()) {
        try {
          millis = Long.parseLong(timeoutMs);
        } catch (final Exception e) {
          LOGGER.warn("Cannot parse consumer timeout", e);
        }
      }
      final Duration timeout = millis > 0 ? Duration.ofMillis(millis) : Duration.ofDays(1000);
      consumeMessages(formatPluginName, avroFormatPlugin, ingestRunData, consumer, timeout);
    }
  }

  protected <T> void consumeMessages(
      final String formatPluginName,
      final GeoWaveAvroFormatPlugin<T, ?> avroFormatPlugin,
      final KafkaIngestRunData ingestRunData,
      final Consumer<byte[], byte[]> consumer,
      final Duration timeout) {
    int currentBatchId = 0;
    final int batchSize = kafkaOptions.getBatchSize();
    try {
      final ConsumerRecords<byte[], byte[]> iterator = consumer.poll(timeout);
      for (final ConsumerRecord<byte[], byte[]> msg : iterator) {
        LOGGER.info("[" + formatPluginName + "] message received");
        final T dataRecord =
            GenericAvroSerializer.deserialize(msg.value(), avroFormatPlugin.getAvroSchema());

        if (dataRecord != null) {
          try {
            processMessage(dataRecord, ingestRunData, avroFormatPlugin);
            if (++currentBatchId > batchSize) {
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format("Flushing %d items", currentBatchId));
              }
              ingestRunData.flush();
              currentBatchId = 0;
            }
          } catch (final Exception e) {
            LOGGER.error("Error processing message: " + e.getMessage(), e);
          }
        }
      }
      // Flush any outstanding items
      if (currentBatchId > 0) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(String.format("Flushing %d items", currentBatchId));
        }
        ingestRunData.flush();
        currentBatchId = 0;
      }
      if (kafkaOptions.isFlushAndReconnect()) {
        LOGGER.info(
            "Consumer timed out from Kafka topic [" + formatPluginName + "]... Reconnecting...");
        consumeMessages(formatPluginName, avroFormatPlugin, ingestRunData, consumer, timeout);
      } else {
        LOGGER.info("Consumer timed out from Kafka topic [" + formatPluginName + "]... ");
      }
    } catch (final Exception e) {
      LOGGER.warn("Consuming from Kafka topic [" + formatPluginName + "] was interrupted... ", e);
    }
  }

  protected synchronized <T> void processMessage(
      final T dataRecord,
      final KafkaIngestRunData ingestRunData,
      final GeoWaveAvroFormatPlugin<T, ?> plugin) throws IOException {

    final IngestPluginBase<T, ?> ingestPlugin = plugin.getIngestWithAvroPlugin();
    final IndexProvider indexProvider = plugin;

    final Map<String, Writer> writerMap = new HashMap<>();
    final Map<String, Index> indexMap = new HashMap<>();

    for (final Index index : indices) {
      indexMap.put(index.getName(), index);
    }

    final Index[] requiredIndices = indexProvider.getRequiredIndices();
    if ((requiredIndices != null) && (requiredIndices.length > 0)) {
      for (final Index requiredIndex : requiredIndices) {
        indexMap.put(requiredIndex.getName(), requiredIndex);
      }
    }

    try (CloseableIterator<?> geowaveDataIt =
        ingestPlugin.toGeoWaveData(dataRecord, indexMap.keySet().toArray(new String[0]))) {
      while (geowaveDataIt.hasNext()) {
        final GeoWaveData<?> geowaveData = (GeoWaveData<?>) geowaveDataIt.next();
        final DataTypeAdapter adapter = ingestRunData.getDataAdapter(geowaveData);
        if (adapter == null) {
          LOGGER.warn("Adapter not found for " + geowaveData.getValue());
          continue;
        }
        Writer indexWriter = writerMap.get(adapter.getTypeName());
        if (indexWriter == null) {
          final List<Index> indexList = new ArrayList<>();
          for (final String indexName : geowaveData.getIndexNames()) {
            final Index index = indexMap.get(indexName);
            if (index == null) {
              LOGGER.warn("Index '" + indexName + "' not found for " + geowaveData.getValue());
              continue;
            }
            indexList.add(index);
          }
          indexWriter =
              ingestRunData.getIndexWriter(
                  adapter,
                  visibilityHandler,
                  indexList.toArray(new Index[indexList.size()]));
          writerMap.put(adapter.getTypeName(), indexWriter);
        }

        indexWriter.write(geowaveData.getValue());
      }
    }
  }

  public List<Future<?>> getFutures() {
    return futures;
  }

  /**
   * @return {@code true} if all futures are complete
   */
  public boolean isComplete() {
    for (final Future<?> future : futures) {
      if (!future.isDone()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Wait for all kafka topics to complete, then return the result objects.
   *
   * @return the future results
   * @throws InterruptedException
   * @throws ExecutionException
   */
  public List<Object> waitFutures() throws InterruptedException, ExecutionException {
    final List<Object> results = new ArrayList<>();
    for (final Future<?> future : futures) {
      results.add(future.get());
    }
    return results;
  }
}
