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
package mil.nga.giat.geowave.core.ingest.kafka;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import mil.nga.giat.geowave.core.ingest.avro.GenericAvroSerializer;

import org.apache.avro.specific.SpecificRecordBase;

/**
 * Default encoder used by Kafka to serialize Avro generated Java object to
 * binary. This class is specified as a property in the Kafka config setup.
 * 
 * Key: serializer.class
 * 
 * Value: mil.nga.giat.geowave.core.ingest.kafka.AvroKafkaEncoder
 * 
 * @param <T>
 *            - Base Avro class extended by all generated class files
 */
public class AvroKafkaEncoder<T extends SpecificRecordBase> implements
		Encoder<T>
{
	private final GenericAvroSerializer<T> serializer = new GenericAvroSerializer<T>();

	public AvroKafkaEncoder(
			final VerifiableProperties verifiableProperties ) {
		// This constructor must be present to avoid runtime errors
	}

	@Override
	public byte[] toBytes(
			final T object ) {
		return serializer.serialize(
				object,
				object.getSchema());
	}
}
