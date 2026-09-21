/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.driver;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * Round-trips an object through the serializations registered under {@code io.serializations}.
 *
 * <p> Hadoop mappers and reducers are free to reuse a single output key and value across every
 * {@code context.write} call, and several of the ones under test here do. Holding on to the
 * references a driver was handed would then give back n copies of whatever the last call left
 * behind, so each output is copied as the real framework would copy it on its way to disk.
 */
final class HadoopSerializationCopier {
  private HadoopSerializationCopier() {}

  @SuppressWarnings("unchecked")
  static <T> T copy(final T source, final Configuration conf) throws IOException {
    if (source == null) {
      return null;
    }
    final Class<T> type = (Class<T>) source.getClass();
    final SerializationFactory factory = new SerializationFactory(conf);
    final Serializer<T> serializer = factory.getSerializer(type);
    final Deserializer<T> deserializer = factory.getDeserializer(type);
    if ((serializer == null) || (deserializer == null)) {
      throw new IOException(
          "No io.serializations entry accepts " + type.getName() + "; a driver cannot copy it");
    }
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    serializer.open(bytes);
    serializer.serialize(source);
    serializer.close();

    deserializer.open(new ByteArrayInputStream(bytes.toByteArray()));
    try {
      return deserializer.deserialize(null);
    } finally {
      deserializer.close();
    }
  }
}
