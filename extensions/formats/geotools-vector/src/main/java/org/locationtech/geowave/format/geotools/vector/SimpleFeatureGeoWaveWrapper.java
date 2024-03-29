/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.geotools.vector;

import java.util.Iterator;
import java.util.List;
import org.geotools.data.DataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.util.SimpleFeatureUserDataConfigurationSet;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.format.geotools.vector.RetypingVectorDataPlugin.RetypingVectorDataSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a wrapper for a GeoTools SimpleFeatureCollection as a convenience to ingest it into
 * GeoWave by translating a list of SimpleFeatureCollection to a closeable iterator of GeoWaveData
 */
public class SimpleFeatureGeoWaveWrapper implements CloseableIterator<GeoWaveData<SimpleFeature>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleFeatureGeoWaveWrapper.class);

  private class InternalIterator implements CloseableIterator<GeoWaveData<SimpleFeature>> {
    private final SimpleFeatureIterator featureIterator;
    private final DataTypeAdapter<SimpleFeature> dataAdapter;
    private RetypingVectorDataSource source = null;
    private final Filter filter;
    private SimpleFeatureBuilder builder = null;
    private GeoWaveData<SimpleFeature> currentData = null;
    private boolean closed = false;

    public InternalIterator(final SimpleFeatureCollection featureCollection, final Filter filter) {
      this.filter = filter;
      featureIterator = featureCollection.features();
      final SimpleFeatureType originalSchema = featureCollection.getSchema();
      SimpleFeatureType retypedSchema =
          SimpleFeatureUserDataConfigurationSet.configureType(originalSchema);
      if (retypingPlugin != null) {
        source = retypingPlugin.getRetypingSource(originalSchema);
        if (source != null) {
          retypedSchema = source.getRetypedSimpleFeatureType();
          builder = new SimpleFeatureBuilder(retypedSchema);
        }
      }
      dataAdapter = new FeatureDataAdapter(retypedSchema);
    }

    @Override
    public boolean hasNext() {
      if (currentData == null) {
        // return a flag indicating if we find more data that matches
        // the filter, essentially peeking and caching the result
        return nextData();
      }
      return true;
    }

    @Override
    public GeoWaveData<SimpleFeature> next() {
      if (currentData == null) {
        // get the next data that matches the filter
        nextData();
      }
      // return that data and set the current data to null
      final GeoWaveData<SimpleFeature> retVal = currentData;
      currentData = null;
      return retVal;
    }

    private synchronized boolean nextData() {
      SimpleFeature nextAcceptedFeature;
      do {
        if (!featureIterator.hasNext()) {
          return false;
        }
        nextAcceptedFeature = featureIterator.next();
        if (builder != null) {
          nextAcceptedFeature = source.getRetypedSimpleFeature(builder, nextAcceptedFeature);
        }
      } while (!filter.evaluate(nextAcceptedFeature));
      currentData = new GeoWaveData<>(dataAdapter, indexNames, nextAcceptedFeature);
      return true;
    }

    @Override
    public void remove() {}

    @Override
    public void close() {
      if (!closed) {
        featureIterator.close();
        closed = true;
      }
    }
  }

  private final List<SimpleFeatureCollection> featureCollections;
  private final String[] indexNames;
  private InternalIterator currentIterator = null;
  private final DataStore dataStore;
  private final RetypingVectorDataPlugin retypingPlugin;
  private final Filter filter;

  public SimpleFeatureGeoWaveWrapper(
      final List<SimpleFeatureCollection> featureCollections,
      final String[] indexNames,
      final DataStore dataStore,
      final RetypingVectorDataPlugin retypingPlugin,
      final Filter filter) {
    this.featureCollections = featureCollections;
    this.indexNames = indexNames;
    this.dataStore = dataStore;
    this.retypingPlugin = retypingPlugin;
    this.filter = filter;
  }

  @Override
  public boolean hasNext() {
    if ((currentIterator == null) || !currentIterator.hasNext()) {
      // return a flag indicating if we find another iterator that hasNext
      return nextIterator();
    }
    // currentIterator has next
    return true;
  }

  private synchronized boolean nextIterator() {
    if (currentIterator != null) {
      currentIterator.close();
    }
    final Iterator<SimpleFeatureCollection> it = featureCollections.iterator();
    while (it.hasNext()) {
      final SimpleFeatureCollection collection = it.next();
      final InternalIterator featureIt = new InternalIterator(collection, filter);

      it.remove();
      if (!featureIt.hasNext()) {
        featureIt.close();
      } else {
        currentIterator = featureIt;
        return true;
      }
    }
    return false;
  }

  @Override
  public GeoWaveData<SimpleFeature> next() {
    if ((currentIterator == null) || !currentIterator.hasNext()) {
      if (nextIterator()) {
        return currentIterator.next();
      }
      return null;
    }
    return currentIterator.next();
  }

  @Override
  public void remove() {
    if (currentIterator != null) {
      // this isn't really implemented anyway and should not be called
      currentIterator.remove();
    }
  }

  @Override
  public void close() {
    if (currentIterator != null) {
      currentIterator.close();
      currentIterator = null;
    }
    if (dataStore != null) {
      dataStore.dispose();
    }
  }
}
