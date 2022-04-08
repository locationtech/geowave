/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.geotools.vector;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.FilenameUtils;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.util.SimpleFeatureUserDataConfigurationSet;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.locationtech.geowave.format.geotools.vector.RetypingVectorDataPlugin.RetypingVectorDataSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This plugin is used for ingesting any GeoTools supported file data store from a local file system
 * directly into GeoWave as GeoTools' SimpleFeatures. It supports the default configuration of
 * spatial and spatial-temporal indices and does NOT currently support the capability to stage
 * intermediate data to HDFS to be ingested using a map-reduce job.
 */
public class GeoToolsVectorDataStoreIngestPlugin implements LocalFileIngestPlugin<SimpleFeature> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoToolsVectorDataStoreIngestPlugin.class);
  private static final String PROPERTIES_EXTENSION = ".properties";

  private final RetypingVectorDataPlugin retypingPlugin;
  private final Filter filter;
  private final List<String> featureTypeNames;

  public GeoToolsVectorDataStoreIngestPlugin(final RetypingVectorDataPlugin retypingPlugin) {
    // by default inherit the types of the original file
    this(retypingPlugin, Filter.INCLUDE, new ArrayList<String>());
  }

  public GeoToolsVectorDataStoreIngestPlugin() {
    this(Filter.INCLUDE);
  }

  public GeoToolsVectorDataStoreIngestPlugin(final Filter filter) {
    // by default inherit the types of the original file
    this(null, filter, new ArrayList<String>());
  }

  public GeoToolsVectorDataStoreIngestPlugin(
      final RetypingVectorDataPlugin retypingPlugin,
      final Filter filter,
      final List<String> featureTypeNames) {
    // this constructor can be used directly as an extension point for
    // retyping the original feature data, if the retyping plugin is null,
    // the data will be ingested as the original type
    this.retypingPlugin = retypingPlugin;
    this.filter = filter;
    this.featureTypeNames = featureTypeNames;
  }

  @Override
  public String[] getFileExtensionFilters() {
    return new String[] {};
  }

  @Override
  public void init(final URL baseDirectory) {}

  private static boolean isPropertiesFile(final URL file) {
    return FilenameUtils.getName(file.getPath()).toLowerCase(Locale.ENGLISH).endsWith(
        PROPERTIES_EXTENSION);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static DataStore getDataStore(final URL file) throws IOException {
    final Map<Object, Object> map = new HashMap<>();
    if (isPropertiesFile(file)) {
      try (InputStream fis = file.openStream()) {
        final Properties prop = new Properties();
        prop.load(fis);
        map.putAll(prop);
        final DataStore dataStore = DataStoreFinder.getDataStore((Map) map);
        return dataStore;
      }
    }
    map.put("url", file);
    if (System.getProperty(StringUtils.GEOWAVE_CHARSET_PROPERTY_NAME) != null) {
      map.put(
          "charset",
          Charset.forName(System.getProperty(StringUtils.GEOWAVE_CHARSET_PROPERTY_NAME)));
    }
    final DataStore dataStore = DataStoreFinder.getDataStore((Map) map);
    return dataStore;
  }

  @Override
  public boolean supportsFile(final URL file) {
    DataStore dataStore = null;
    try {
      dataStore = getDataStore(file);
      if (dataStore != null) {
        dataStore.dispose();
      }
    } catch (final Exception e) {
      LOGGER.info("GeoTools was unable to read data source for file '" + file.getPath() + "'", e);
    }
    return dataStore != null;
  }

  @Override
  public DataTypeAdapter<SimpleFeature>[] getDataAdapters() {
    return new FeatureDataAdapter[] {};
  }

  @Override
  public DataTypeAdapter<SimpleFeature>[] getDataAdapters(final URL url) {
    DataStore dataStore = null;
    try {
      dataStore = getDataStore(url);
    } catch (final Exception e) {
      LOGGER.error("Exception getting a datastore instance", e);
    }
    if (dataStore != null) {
      final List<SimpleFeatureCollection> featureCollections =
          getFeatureCollections(dataStore, url);
      return featureCollections.stream().map(featureCollection -> {
        final SimpleFeatureType originalSchema = featureCollection.getSchema();
        SimpleFeatureType retypedSchema =
            SimpleFeatureUserDataConfigurationSet.configureType(originalSchema);
        if (retypingPlugin != null) {
          final RetypingVectorDataSource source = retypingPlugin.getRetypingSource(originalSchema);
          if (source != null) {
            retypedSchema = source.getRetypedSimpleFeatureType();
          }
        }
        return new FeatureDataAdapter(retypedSchema);
      }).toArray(FeatureDataAdapter[]::new);
    }

    LOGGER.error("Unable to get a datastore instance, getDataStore returned null");
    return null;
  }

  @Override
  public CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveData(
      final URL input,
      final String[] indexNames) {
    DataStore dataStore = null;
    try {
      dataStore = getDataStore(input);
    } catch (final Exception e) {
      LOGGER.error("Exception getting a datastore instance", e);
    }
    if (dataStore != null) {
      final List<SimpleFeatureCollection> featureCollections =
          getFeatureCollections(dataStore, input);
      if (featureCollections == null) {
        return null;
      }
      return new SimpleFeatureGeoWaveWrapper(
          featureCollections,
          indexNames,
          dataStore,
          retypingPlugin,
          filter);
    }

    LOGGER.error("Unable to get a datastore instance, getDataStore returned null");
    return null;
  }

  private List<SimpleFeatureCollection> getFeatureCollections(
      final DataStore dataStore,
      final URL url) {
    List<Name> names = null;
    try {
      names = dataStore.getNames();
    } catch (final IOException e) {
      LOGGER.error("Unable to get feature types from datastore '" + url.getPath() + "'", e);
    }
    if (names == null) {
      LOGGER.error("Unable to get datatore name");
      return null;
    }
    final List<SimpleFeatureCollection> featureCollections = new ArrayList<>();
    for (final Name name : names) {
      try {
        if ((featureTypeNames != null)
            && !featureTypeNames.isEmpty()
            && !featureTypeNames.contains(name.getLocalPart())) {
          continue;
        }
        final SimpleFeatureSource source = dataStore.getFeatureSource(name);

        final SimpleFeatureCollection featureCollection;
        // we pass the filter in here so that the datastore may be
        // able to take advantage of the filter
        // but also send the filter along to be evaluated per
        // feature in case the filter is not respected by the
        // underlying store, we may want to consider relying on the
        // filtering being done by the store here
        if (filter != null) {
          featureCollection = source.getFeatures(filter);
        } else {
          featureCollection = source.getFeatures();
        }
        featureCollections.add(featureCollection);
      } catch (final Exception e) {
        LOGGER.error("Unable to ingest data source for feature name '" + name + "'", e);
      }
    }
    return featureCollections;
  }

  @Override
  public Index[] getRequiredIndices() {
    return new Index[] {};
  }

  @Override
  public String[] getSupportedIndexTypes() {
    return new String[] {SpatialField.DEFAULT_GEOMETRY_FIELD_NAME, TimeField.DEFAULT_FIELD_ID};
  }
}
