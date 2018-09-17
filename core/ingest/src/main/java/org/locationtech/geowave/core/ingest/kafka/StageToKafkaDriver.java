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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;
import org.locationtech.geowave.core.ingest.avro.AvroFormatPlugin;
import org.locationtech.geowave.core.ingest.local.AbstractLocalFileDriver;
import org.locationtech.geowave.core.ingest.local.LocalInputCommandLineOptions;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

/**
 * This class actually executes the staging of data to a Kafka topic based on
 * the available type plugin providers that are discovered through SPI.
 */
public class StageToKafkaDriver<T extends SpecificRecordBase> extends
		AbstractLocalFileDriver<AvroFormatPlugin<?, ?>, StageKafkaData<?>>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(StageToKafkaDriver.class);

	private final Map<String, AvroFormatPlugin<?, ?>> ingestPlugins;
	private final KafkaProducerCommandLineOptions kafkaOptions;

	public StageToKafkaDriver(
			KafkaProducerCommandLineOptions kafkaOptions,
			Map<String, AvroFormatPlugin<?, ?>> ingestPlugins,
			LocalInputCommandLineOptions localOptions ) {
		super(
				localOptions);
		this.kafkaOptions = kafkaOptions;
		this.ingestPlugins = ingestPlugins;
	}

	@Override
	protected void processFile(
			final URL file,
			final String typeName,
			final AvroFormatPlugin<?, ?> plugin,
			final StageKafkaData<?> runData ) {

		try {
			final Producer<String, Object> producer = (Producer<String, Object>) runData.getProducer(
					typeName,
					plugin);
			try (final CloseableIterator<?> avroRecords = plugin.toAvroObjects(file)) {
				while (avroRecords.hasNext()) {
					final Object avroRecord = avroRecords.next();
					final KeyedMessage<String, Object> data = new KeyedMessage<String, Object>(
							typeName,
							avroRecord);
					producer.send(data);
				}
			}
		}
		catch (final Exception e) {
			LOGGER.info(
					"Unable to send file [" + file.getPath() + "] to Kafka topic: " + e.getMessage(),
					e);
		}
	}

	public boolean runOperation(
			String inputPath,
			File configFile ) {

		final Map<String, AvroFormatPlugin<?, ?>> stageToKafkaPlugins = ingestPlugins;

		try {
			final StageKafkaData<T> runData = new StageKafkaData<T>(
					kafkaOptions.getProperties());
			processInput(
					inputPath,
					configFile,
					stageToKafkaPlugins,
					runData);
			runData.close();
			return true;
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to process input",
					e);
			return false;
		}

	}
}
