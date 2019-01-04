/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.locationtech.geowave.adapter.vector.avro.AvroAttributeValues;
import org.locationtech.geowave.adapter.vector.avro.AvroFeatureDefinition;
import org.locationtech.geowave.adapter.vector.avro.AvroSimpleFeature;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoWaveAvroFeatureWriter implements FieldWriter<SimpleFeature, Object> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveAvroFeatureWriter.class);

  private final EncoderFactory ef = EncoderFactory.get();

  private final SpecificDatumWriter<AvroSimpleFeature> datumWriter = new SpecificDatumWriter<>();

  @Override
  public byte[] getVisibility(
      final SimpleFeature rowValue,
      final String fieldName,
      final Object fieldValue) {
    return new byte[] {};
  }

  @Override
  public byte[] writeField(final Object fieldValue) {
    byte[] serializedAttributes = null;
    try {
      serializedAttributes = serializeAvroSimpleFeature((SimpleFeature) fieldValue, null, null, "");
    } catch (final IOException e) {
      e.printStackTrace();
      LOGGER.error(
          "Error, failed to serialize SimpleFeature with id '"
              + ((SimpleFeature) fieldValue).getID()
              + "'",
          e);
    }

    // there is no need to preface the payload with the class name and a
    // length of the class name, the implementation is assumed to be known
    // on read so we can save space on persistence
    return serializedAttributes;
  }

  // Serialization logic

  /**
   * *
   *
   * @param avroObject Avro object to serialized
   * @return byte array of serialized avro data
   * @throws IOException
   */
  private byte[] serializeAvroSimpleFeature(final AvroSimpleFeature avroObject) throws IOException {
    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    final BinaryEncoder encoder = ef.binaryEncoder(os, null);
    datumWriter.setSchema(avroObject.getSchema());
    datumWriter.write(avroObject, encoder);
    encoder.flush();
    return os.toByteArray();
  }

  /**
   * * Converts a SimpleFeature to an avroSimpleFeature and then serializes it.
   *
   * @param sf Simple Feature to be serialized
   * @param avroObjectToReuse null or AvroSimpleFeature instance to be re-used. If null a new
   *        instance will be allocated
   * @param defaultClassifications null map of attribute names vs. classification. if null all
   *        values will be set to the default classification
   * @param defaultClassification null or default classification. if null and defaultClassifications
   *        are not provided an exception will be thrown
   * @return
   * @throws IOException
   */
  public byte[] serializeAvroSimpleFeature(
      final SimpleFeature sf,
      AvroSimpleFeature avroObjectToReuse,
      final Map<String, String> defaultClassifications,
      final String defaultClassification) throws IOException {
    if (sf == null) {
      throw new IOException("Feature cannot be null");
    }

    if ((defaultClassification == null) && (defaultClassifications == null)) {
      throw new IOException(
          "if per attribute classifications aren't provided then a default classification must be provided");
    }

    final SimpleFeatureType sft = sf.getType();
    if (avroObjectToReuse == null) {
      avroObjectToReuse = new AvroSimpleFeature();
    }

    final AvroFeatureDefinition fd =
        GeoWaveAvroFeatureUtils.buildFeatureDefinition(
            avroObjectToReuse.getFeatureType(),
            sft,
            defaultClassifications,
            defaultClassification);
    avroObjectToReuse.setFeatureType(fd);

    final AvroAttributeValues av = GeoWaveAvroFeatureUtils.buildAttributeValue(sf, sft);
    avroObjectToReuse.setValue(av);

    return serializeAvroSimpleFeature(avroObjectToReuse);
  }
}
