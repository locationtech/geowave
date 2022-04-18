/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples.ingest.plugin;

import java.io.File;
import java.net.URISyntaxException;
import org.locationtech.geowave.core.geotime.index.api.SpatialIndexBuilder;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IngestOptions;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.opengis.feature.simple.SimpleFeature;


/**
 * This class provides an example of how to ingest data that's in a non-standard format using a
 * custom ingest plugin that transforms the data into SimpleFeatures.
 */
public class CustomIngestPluginExample {

  private DataStore dataStore;
  private Index spatialIndex;

  public static void main(final String[] args) throws URISyntaxException {
    final CustomIngestPluginExample example = new CustomIngestPluginExample();
    example.run();
  }

  public void run() throws URISyntaxException {
    // Create an in-memory data store to use with this example
    dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());

    // Create the spatial index
    spatialIndex = new SpatialIndexBuilder().createIndex();

    // Configure ingest options to use our custom plugin
    final IngestOptions.Builder<SimpleFeature> ingestOptions = IngestOptions.newBuilder();

    // Set our custom ingest plugin as the format to use for the ingest
    ingestOptions.format(new CustomIngestPlugin());

    // Get the path of the geonames text file from the example resources
    final File geonamesFile =
        new File(CustomIngestPlugin.class.getClassLoader().getResource("geonames.txt").toURI());

    // Ingest the data
    dataStore.ingest(geonamesFile.getAbsolutePath(), ingestOptions.build(), spatialIndex);

    // Perform a query on the data
    try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(null)) {
      while (iterator.hasNext()) {
        System.out.println("Query match: " + iterator.next().getAttribute("Location"));
      }
    }
  }

}
