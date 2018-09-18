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
package org.locationtech.geowave.core.ingest.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.locationtech.geowave.core.ingest.avro.AvroFormatPlugin;
import org.locationtech.geowave.core.ingest.avro.GenericAvroSerializer;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.IndexPluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.VisibilityOptions;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.ingest.IndexProvider;
import org.locationtech.geowave.core.store.ingest.IngestPluginBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * This class executes the ingestion of intermediate data from a Kafka topic
 * into GeoWave.
 *
 */
public class IngestFromKafkaDriver
{
	private final static Logger LOGGER = LoggerFactory.getLogger(IngestFromKafkaDriver.class);

	private final DataStorePluginOptions storeOptions;
	private final List<IndexPluginOptions> indexOptions;
	private final Map<String, AvroFormatPlugin<?, ?>> ingestPlugins;
	private final KafkaConsumerCommandLineOptions kafkaOptions;
	private final VisibilityOptions ingestOptions;
	private final List<Future<?>> futures = new ArrayList<>();

	public IngestFromKafkaDriver(
			final DataStorePluginOptions storeOptions,
			final List<IndexPluginOptions> indexOptions,
			final Map<String, AvroFormatPlugin<?, ?>> ingestPlugins,
			final KafkaConsumerCommandLineOptions kafkaOptions,
			final VisibilityOptions ingestOptions ) {
		this.storeOptions = storeOptions;
		this.indexOptions = indexOptions;
		this.ingestPlugins = ingestPlugins;
		this.kafkaOptions = kafkaOptions;
		this.ingestOptions = ingestOptions;
	}

	public boolean runOperation() {

		final DataStore dataStore = storeOptions.createDataStore();

		final List<String> queue = new ArrayList<>();
		addPluginsToQueue(
				ingestPlugins,
				queue);

		configureAndLaunchPlugins(
				dataStore,
				ingestPlugins,
				queue);

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
			}
			catch (final InterruptedException e) {
				LOGGER.error(
						"Thread interrupted",
						e);
			}
			counter++;
		}

		if (queue.size() == 0) {
			LOGGER.info("All format plugins are now listening on Kafka topics");
		}
		else {
			LOGGER.warn("Unable to setup Kafka consumers for the following format plugins:");
			for (final String formatPluginName : queue) {
				LOGGER.warn("\t[" + formatPluginName + "]");
			}
			return false;
		}
		return true;
	}

	private void addPluginsToQueue(
			final Map<String, AvroFormatPlugin<?, ?>> pluginProviders,
			final List<String> queue ) {
		queue.addAll(pluginProviders.keySet());
	}

	private void configureAndLaunchPlugins(
			final DataStore dataStore,
			final Map<String, AvroFormatPlugin<?, ?>> pluginProviders,
			final List<String> queue ) {
		try {
			for (final Entry<String, AvroFormatPlugin<?, ?>> pluginProvider : pluginProviders.entrySet()) {
				final List<DataTypeAdapter<?>> adapters = new ArrayList<>();

				AvroFormatPlugin<?, ?> avroFormatPlugin = null;
				try {
					avroFormatPlugin = pluginProvider.getValue();

					final IngestPluginBase<?, ?> ingestWithAvroPlugin = avroFormatPlugin.getIngestWithAvroPlugin();
					final DataTypeAdapter<?>[] dataAdapters = ingestWithAvroPlugin.getDataAdapters(ingestOptions
							.getVisibility());
					adapters.addAll(Arrays.asList(dataAdapters));
					final KafkaIngestRunData runData = new KafkaIngestRunData(
							adapters,
							dataStore);

					futures.add(launchTopicConsumer(
							pluginProvider.getKey(),
							avroFormatPlugin,
							runData,
							queue));
				}
				catch (final UnsupportedOperationException e) {
					LOGGER.warn(
							"Plugin provider '" + pluginProvider.getKey() + "' does not support ingest from Kafka",
							e);
					continue;
				}
			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Error in accessing Kafka stream",
					e);
		}
	}

	private ConsumerConnector buildKafkaConsumer() {

		final Properties kafkaProperties = kafkaOptions.getProperties();

		final ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(
				kafkaProperties));

		return consumer;
	}

	private Future<?> launchTopicConsumer(
			final String formatPluginName,
			final AvroFormatPlugin<?, ?> avroFormatPlugin,
			final KafkaIngestRunData ingestRunData,
			final List<String> queue )
			throws IllegalArgumentException {
		final ExecutorService executorService = Executors.newFixedThreadPool(queue.size());
		return executorService.submit(new Runnable() {

			@Override
			public void run() {
				try {
					consumeFromTopic(
							formatPluginName,
							avroFormatPlugin,
							ingestRunData,
							queue);
				}
				catch (final Exception e) {
					LOGGER.error(
							"Error consuming from Kafka topic [" + formatPluginName + "]",
							e);
				}
			}
		});
	}

	public <T> void consumeFromTopic(
			final String formatPluginName,
			final AvroFormatPlugin<T, ?> avroFormatPlugin,
			final KafkaIngestRunData ingestRunData,
			final List<String> queue ) {

		final ConsumerConnector consumer = buildKafkaConsumer();
		if (consumer == null) {
			throw new RuntimeException(
					"Kafka consumer connector is null, unable to create message streams");
		}
		try {
			LOGGER.debug("Kafka consumer setup for format [" + formatPluginName + "] against topic ["
					+ formatPluginName + "]");
			final Map<String, Integer> topicCount = new HashMap<>();
			topicCount.put(
					formatPluginName,
					1);

			final Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer
					.createMessageStreams(topicCount);
			final List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(formatPluginName);

			queue.remove(formatPluginName);
			consumeMessages(
					formatPluginName,
					avroFormatPlugin,
					ingestRunData,
					streams.get(0));
		}
		finally {
			consumer.shutdown();
		}
	}

	protected <T> void consumeMessages(
			final String formatPluginName,
			final AvroFormatPlugin<T, ?> avroFormatPlugin,
			final KafkaIngestRunData ingestRunData,
			final KafkaStream<byte[], byte[]> stream ) {
		int currentBatchId = 0;
		final int batchSize = kafkaOptions.getBatchSize();
		try {
			final ConsumerIterator<byte[], byte[]> messageIterator = stream.iterator();
			while (messageIterator.hasNext()) {
				final byte[] msg = messageIterator.next().message();
				LOGGER.info("[" + formatPluginName + "] message received");
				final T dataRecord = GenericAvroSerializer.deserialize(
						msg,
						avroFormatPlugin.getAvroSchema());

				if (dataRecord != null) {
					try {
						processMessage(
								dataRecord,
								ingestRunData,
								avroFormatPlugin);
						if (++currentBatchId > batchSize) {
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug(String.format(
										"Flushing %d items",
										currentBatchId));
							}
							ingestRunData.flush();
							currentBatchId = 0;
						}
					}
					catch (final Exception e) {
						LOGGER.error(
								"Error processing message: " + e.getMessage(),
								e);
					}
				}
			}
		}
		catch (final ConsumerTimeoutException te) {
			// Flush any outstanding items
			if (currentBatchId > 0) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(String.format(
							"Flushing %d items",
							currentBatchId));
				}
				ingestRunData.flush();
				currentBatchId = 0;
			}
			if (kafkaOptions.isFlushAndReconnect()) {
				LOGGER.info(
						"Consumer timed out from Kafka topic [" + formatPluginName + "]... Reconnecting...",
						te);
				consumeMessages(
						formatPluginName,
						avroFormatPlugin,
						ingestRunData,
						stream);
			}
			else {
				LOGGER.info(
						"Consumer timed out from Kafka topic [" + formatPluginName + "]... ",
						te);
			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Consuming from Kafka topic [" + formatPluginName + "] was interrupted... ",
					e);
		}

	}

	synchronized protected <T> void processMessage(
			final T dataRecord,
			final KafkaIngestRunData ingestRunData,
			final AvroFormatPlugin<T, ?> plugin )
			throws IOException {

		final IngestPluginBase<T, ?> ingestPlugin = plugin.getIngestWithAvroPlugin();
		final IndexProvider indexProvider = plugin;

		final Map<String, Writer> writerMap = new HashMap<>();
		final Map<String, Index> indexMap = new HashMap<>();

		for (final IndexPluginOptions indexOption : indexOptions) {
			final Index primaryIndex = indexOption.createIndex();
			if (primaryIndex == null) {
				LOGGER.error("Could not get index instance, getIndex() returned null;");
				throw new IOException(
						"Could not get index instance, getIndex() returned null");
			}
			indexMap.put(
					primaryIndex.getName(),
					primaryIndex);
		}

		final Index[] requiredIndices = indexProvider.getRequiredIndices();
		if ((requiredIndices != null) && (requiredIndices.length > 0)) {
			for (final Index requiredIndex : requiredIndices) {
				indexMap.put(
						requiredIndex.getName(),
						requiredIndex);
			}
		}

		try (CloseableIterator<?> geowaveDataIt = ingestPlugin.toGeoWaveData(
				dataRecord,
				indexMap.keySet().toArray(
						new String[0]),
				ingestOptions.getVisibility())) {
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
					indexWriter = ingestRunData.getIndexWriter(
							adapter,
							indexList.toArray(new Index[indexList.size()]));
					writerMap.put(
							adapter.getTypeName(),
							indexWriter);
				}

				indexWriter.write(geowaveData.getValue());

			}
		}
	}

	public List<Future<?>> getFutures() {
		return futures;
	}

	/**
	 * Only true if all futures are complete.
	 *
	 * @return
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
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public List<Object> waitFutures()
			throws InterruptedException,
			ExecutionException {
		final List<Object> results = new ArrayList<>();
		for (final Future<?> future : futures) {
			results.add(future.get());
		}
		return results;
	}
}
