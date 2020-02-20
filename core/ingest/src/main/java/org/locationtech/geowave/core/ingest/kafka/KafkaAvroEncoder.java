/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.locationtech.geowave.core.ingest.avro.GenericAvroSerializer;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/**
 * Default encoder used by Kafka to serialize Avro generated Java object to binary. This class is
 * specified as a property in the Kafka config setup.
 *
 * <p> Key: serializer.class
 *
 * <p> Value: org.locationtech.geowave.core.ingest.kafka.AvroKafkaEncoder
 *
 * @param <T> - Base Avro class extended by all generated class files
 */
public class KafkaAvroEncoder<T extends SpecificRecordBase> implements Encoder<T> {
  private final GenericAvroSerializer<T> serializer = new GenericAvroSerializer<>();

  public KafkaAvroEncoder(final VerifiableProperties verifiableProperties) {
    // This constructor must be present to avoid runtime errors
  }

  @Override
  public byte[] toBytes(final T object) {
    return GenericAvroSerializer.serialize(object, object.getSchema());
  }
}
