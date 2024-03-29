/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.geotools.data.FeatureWriter;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.util.Utilities;
import org.locationtech.geowave.adapter.vector.plugin.transaction.GeoWaveTransaction;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class wraps a geotools data store as well as one for statistics (for example to display
 * Heatmaps) into a GeoTools FeatureReader for simple feature data. It acts as a helper for
 * GeoWave's GeoTools data store.
 */
public class GeoWaveFeatureWriter implements FeatureWriter<SimpleFeatureType, SimpleFeature> {

  private SimpleFeature original = null;
  private SimpleFeature live = null;
  private final GeoWaveTransaction transaction;
  private final GeoWaveFeatureReader myReader;
  private final SimpleFeatureType featureType;

  public GeoWaveFeatureWriter(
      final GeoWaveDataStoreComponents components,
      final GeoWaveTransaction transaction,
      final GeoWaveFeatureReader reader) {
    components.initForWrite();
    this.transaction = transaction;
    myReader = reader;
    featureType = components.getFeatureType();
  }

  @Override
  public void close() throws IOException {}

  @Override
  public SimpleFeatureType getFeatureType() {
    return featureType;
  }

  @Override
  public boolean hasNext() throws IOException {
    return ((myReader != null) && myReader.hasNext());
  }

  @Override
  public SimpleFeature next() throws IOException, IllegalArgumentException, NoSuchElementException {
    if (hasNext()) {
      original = myReader.next();
      final List<AttributeDescriptor> descriptors = featureType.getAttributeDescriptors();
      final Object[] defaults = new Object[descriptors.size()];
      int p = 0;
      for (final AttributeDescriptor descriptor : descriptors) {
        defaults[p++] = original.getAttribute(descriptor.getName());
      }
      live = SimpleFeatureBuilder.build(featureType, defaults, original.getID());
    } else {
      original = null;
      final List<AttributeDescriptor> descriptors = featureType.getAttributeDescriptors();
      final Object[] defaults = new Object[descriptors.size()];
      int p = 0;
      for (final AttributeDescriptor descriptor : descriptors) {
        defaults[p++] = descriptor.getDefaultValue();
      }

      live = SimpleFeatureBuilder.build(featureType, defaults, UUID.randomUUID().toString());
    }
    return live;
  }

  @Override
  public void remove() throws IOException {
    transaction.remove(live.getID(), live);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveFeatureWriter.class);

  @Override
  public void write() throws IOException {
    if (live == null) {
      LOGGER.error("Unable to process transaction " + transaction.toString());
      throw new IOException("No current feature to write");
    }

    if (original == null) {
      transaction.add(live.getID(), live);
    } else if (!Utilities.deepEquals(live, original)) {
      transaction.modify(live.getID(), original, live);
    }
    original = null;
    live = null;
  }
}
