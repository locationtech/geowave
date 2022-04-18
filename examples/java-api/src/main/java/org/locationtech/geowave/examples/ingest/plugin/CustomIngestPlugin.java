/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples.ingest.plugin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geowave.adapter.vector.ingest.MinimalSimpleFeatureIngestPlugin;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.examples.ingest.bulk.GeonamesSimpleFeatureType;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * The ingest plugin does the work of translating a URL to a set of SimpleFeatures that can be
 * ingested into GeoWave. While the ingest system offers options for Avro serialization and ingest
 * from HDFS, the {@link MinimalSimpleFeatureIngestPlugin} is the simplest way to create a plugin
 * that ingests data that uses a custom format from the local file system.
 *
 * For examples of more complex ingest plugins, including ones that support Avro serialization and
 * mapreduce ingest, see the formats that are built in to GeoWave.
 */
public class CustomIngestPlugin extends MinimalSimpleFeatureIngestPlugin {

  /**
   * Overriding this method allows the plugin to automatically disregard any file that does not
   * match the given file extension. This can be useful as an early-out to avoid having to perform
   * further processing on the file to see if it's supported. If this method is not overriden, all
   * files that match the ingest URL will be checked for support.
   */
  @Override
  public String[] getFileExtensionFilters() {
    return new String[] {"txt"};
  }

  /**
   * In this example, we'll just assume that the provided file is valid for this format if we are
   * able to parse the first line as a valid entry. All files in the ingest directory that match the
   * file extension filters will be passed through this function before being processed.
   */
  @Override
  public boolean supportsFile(final URL file) {
    try {
      try (final GeonamesFeatureReader reader = new GeonamesFeatureReader(file)) {
        reader.hasNext();
      }
    } catch (final IOException | RuntimeException e) {
      return false;
    }
    return true;
  }

  /**
   * Return all feature types that will be used by the plugin.
   */
  @Override
  protected SimpleFeatureType[] getTypes() {
    return new SimpleFeatureType[] {GeonamesSimpleFeatureType.getInstance()};
  }

  /**
   * Return all of the features from the given URL
   */
  @Override
  protected CloseableIterator<SimpleFeature> getFeatures(final URL input) {
    try {
      return new GeonamesFeatureReader(input);
    } catch (final IOException e) {
      throw new RuntimeException("Unable to read features from URL " + input.toString() + ".", e);
    }
  }


  /**
   * This class reads features line by line from a text file and converts them to SimpleFeatures.
   */
  private static class GeonamesFeatureReader implements CloseableIterator<SimpleFeature> {

    private final BufferedReader reader;
    private SimpleFeature next = null;
    private final SimpleFeatureBuilder builder =
        new SimpleFeatureBuilder(GeonamesSimpleFeatureType.getInstance());

    public GeonamesFeatureReader(final URL input) throws IOException {
      final InputStream inputStream = input.openStream();
      final InputStreamReader inputStreamReader =
          new InputStreamReader(inputStream, StringUtils.UTF8_CHARSET);
      reader = new BufferedReader(inputStreamReader);
    }

    private SimpleFeature parseEntry(final String entry) {
      final String[] tokens = entry.split("\\t"); // Exported Geonames entries are tab-delimited

      final String location = tokens[1];
      final double latitude = Double.parseDouble(tokens[4]);
      final double longitude = Double.parseDouble(tokens[5]);

      builder.set(
          "geometry",
          GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude)));
      builder.set("Latitude", latitude);
      builder.set("Longitude", longitude);
      builder.set("Location", location);

      return builder.buildFeature(tokens[0]);
    }



    private void computeNext() {
      try {
        final String nextLine = reader.readLine();
        if (nextLine != null) {
          next = parseEntry(nextLine);
        }
      } catch (final IOException e) {
        throw new RuntimeException("Encountered an error while reading Geonames.", e);
      }
    }

    @Override
    public boolean hasNext() {
      if (next == null) {
        computeNext();
      }
      return next != null;
    }

    @Override
    public SimpleFeature next() {
      if (next == null) {
        computeNext();
      }
      final SimpleFeature retValue = next;
      next = null;
      return retValue;
    }

    @Override
    public void close() {
      try {
        reader.close();
      } catch (final IOException e) {
        throw new RuntimeException("Encountered an error while closing Geonames file.", e);
      }
    }

  }

}
