/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.avro;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.GeoWaveAvroFeatureUtils;
import org.locationtech.geowave.adapter.vector.avro.AvroAttributeValues;
import org.locationtech.geowave.adapter.vector.avro.AvroFeatureDefinition;
import org.locationtech.geowave.adapter.vector.avro.AvroSimpleFeatureCollection;
import org.locationtech.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import org.locationtech.geowave.core.ingest.hdfs.mapreduce.IngestWithReducer;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIterator.Wrapper;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.ingest.IngestPluginBase;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.internal.Maps;

/**
 * This plugin is used for ingesting any GPX formatted data from a local file system into GeoWave as
 * GeoTools' SimpleFeatures. It supports the default configuration of spatial and spatial-temporal
 * indices and it will support wither directly ingesting GPX data from a local file system to
 * GeoWave or to stage the data in an intermediate format in HDFS and then to ingest it into GeoWave
 * using a map-reduce job. It supports OSM metadata.xml files if the file is directly in the root
 * base directory that is passed in command-line to the ingest framework.
 */
public class GeoWaveAvroIngestPlugin extends
    AbstractSimpleFeatureIngestPlugin<AvroSimpleFeatureCollection> {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveAvroIngestPlugin.class);

  public GeoWaveAvroIngestPlugin() {}

  @Override
  public String[] getFileExtensionFilters() {
    return new String[] {"avro", "dat", "bin", "json" // TODO: does the Avro DataFileReader actually
        // support JSON
        // formatted avro files, or should we limit the extensions
        // to expected binary extensions?
    };
  }

  @Override
  public void init(final URL baseDirectory) {}

  @Override
  public boolean supportsFile(final URL file) {

    try (DataFileStream<AvroSimpleFeatureCollection> ds =
        new DataFileStream<>(
            file.openStream(),
            new SpecificDatumReader<AvroSimpleFeatureCollection>(
                AvroSimpleFeatureCollection.getClassSchema()))) {
      if (ds.getHeader() != null) {
        return true;
      }
    } catch (final IOException e) {
      // just log as info as this may not have been intended to be read as
      // avro vector data
      LOGGER.info("Unable to read file as Avro vector data '" + file.getPath() + "'", e);
    }

    return false;
  }

  @Override
  protected SimpleFeatureType[] getTypes() {
    return new SimpleFeatureType[] {};
  }

  @Override
  public Schema getAvroSchema() {
    return AvroSimpleFeatureCollection.getClassSchema();
  }

  @Override
  public CloseableIterator<AvroSimpleFeatureCollection> toAvroObjects(final URL input) {
    try {
      final DataFileStream<AvroSimpleFeatureCollection> reader =
          new DataFileStream<>(
              input.openStream(),
              new SpecificDatumReader<AvroSimpleFeatureCollection>(
                  AvroSimpleFeatureCollection.getClassSchema()));

      return new CloseableIterator<AvroSimpleFeatureCollection>() {

        @Override
        public boolean hasNext() {
          return reader.hasNext();
        }

        @Override
        public AvroSimpleFeatureCollection next() {
          return reader.next();
        }

        @Override
        public void close() {
          try {
            reader.close();
          } catch (final IOException e) {
            LOGGER.warn("Unable to close file '" + input.getPath() + "'", e);
          }
        }
      };
    } catch (final IOException e) {
      LOGGER.warn(
          "Unable to read file '" + input.getPath() + "' as AVRO SimpleFeatureCollection",
          e);
    }
    return new CloseableIterator.Empty<>();
  }

  @Override
  public boolean isUseReducerPreferred() {
    return false;
  }

  @Override
  public IngestWithMapper<AvroSimpleFeatureCollection, SimpleFeature> ingestWithMapper() {
    return new IngestAvroFeaturesFromHdfs(this);
  }

  @Override
  public IngestWithReducer<AvroSimpleFeatureCollection, ?, ?, SimpleFeature> ingestWithReducer() {
    // unsupported right now
    throw new UnsupportedOperationException(
        "Avro simple feature collections cannot be ingested with a reducer");
  }

  @Override
  public DataTypeAdapter<SimpleFeature>[] getDataAdapters(final URL url) {
    final Map<String, FeatureDataAdapter> adapters = Maps.newHashMap();
    try (final CloseableIterator<AvroSimpleFeatureCollection> avroObjects = toAvroObjects(url)) {
      while (avroObjects.hasNext()) {
        final AvroFeatureDefinition featureDefinition = avroObjects.next().getFeatureType();
        try {
          final SimpleFeatureType featureType =
              GeoWaveAvroFeatureUtils.avroFeatureDefinitionToGTSimpleFeatureType(featureDefinition);
          final FeatureDataAdapter adapter = new FeatureDataAdapter(featureType);
          adapters.put(adapter.getTypeName(), adapter);
        } catch (final ClassNotFoundException e) {
          LOGGER.warn("Unable to read simple feature type from Avro", e);
        }
      }
    }
    return adapters.values().toArray(new FeatureDataAdapter[adapters.size()]);

  }

  @Override
  protected CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
      final AvroSimpleFeatureCollection featureCollection,
      final String[] indexNames) {
    final AvroFeatureDefinition featureDefinition = featureCollection.getFeatureType();
    final List<GeoWaveData<SimpleFeature>> retVal = new ArrayList<>();
    SimpleFeatureType featureType;
    try {
      featureType =
          GeoWaveAvroFeatureUtils.avroFeatureDefinitionToGTSimpleFeatureType(featureDefinition);

      final FeatureDataAdapter adapter = new FeatureDataAdapter(featureType);
      final List<String> attributeTypes = featureDefinition.getAttributeTypes();
      for (final AvroAttributeValues attributeValues : featureCollection.getSimpleFeatureCollection()) {
        try {
          final SimpleFeature simpleFeature =
              GeoWaveAvroFeatureUtils.avroSimpleFeatureToGTSimpleFeature(
                  featureType,
                  attributeTypes,
                  attributeValues);
          retVal.add(new GeoWaveData<>(adapter, indexNames, simpleFeature));
        } catch (final Exception e) {
          LOGGER.warn("Unable to read simple feature from Avro", e);
        }
      }
    } catch (final ClassNotFoundException e) {
      LOGGER.warn("Unable to read simple feature type from Avro", e);
    }
    return new Wrapper<>(retVal.iterator());
  }

  @Override
  public Index[] getRequiredIndices() {
    return new Index[] {};
  }

  public static class IngestAvroFeaturesFromHdfs extends
      AbstractIngestSimpleFeatureWithMapper<AvroSimpleFeatureCollection> {
    public IngestAvroFeaturesFromHdfs() {
      this(new GeoWaveAvroIngestPlugin());
      // this constructor will be used when deserialized
    }

    public IngestAvroFeaturesFromHdfs(final GeoWaveAvroIngestPlugin parentPlugin) {
      super(parentPlugin);
    }

    @Override
    public String[] getSupportedIndexTypes() {
      return new String[] {SpatialField.DEFAULT_GEOMETRY_FIELD_NAME, TimeField.DEFAULT_FIELD_ID};
    }
  }

  @Override
  public IngestPluginBase<AvroSimpleFeatureCollection, SimpleFeature> getIngestWithAvroPlugin() {
    return new IngestAvroFeaturesFromHdfs(this);
  }

  @Override
  public String[] getSupportedIndexTypes() {
    return new String[] {SpatialField.DEFAULT_GEOMETRY_FIELD_NAME, TimeField.DEFAULT_FIELD_ID};
  }

}
