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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.geowave.adapter.vector.index.IndexQueryStrategySPI.QueryHint;
import org.locationtech.geowave.adapter.vector.plugin.transaction.GeoWaveTransaction;
import org.locationtech.geowave.adapter.vector.plugin.transaction.StatisticsCache;
import org.locationtech.geowave.adapter.vector.plugin.transaction.TransactionsAllocator;
import org.locationtech.geowave.core.geotime.store.InternalGeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCrsIndexModel;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.SpatialIndexUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import com.google.common.collect.Maps;

public class GeoWaveDataStoreComponents {
  private final InternalGeotoolsFeatureDataAdapter adapter;
  private final DataStore dataStore;
  private final IndexStore indexStore;
  private final DataStatisticsStore dataStatisticsStore;
  private final AdapterIndexMappingStore indexMappingStore;
  private final GeoWaveGTDataStore gtStore;
  private final TransactionsAllocator transactionAllocator;
  private CoordinateReferenceSystem crs = null;
  private final SimpleFeatureType featureType;

  private final Index[] adapterIndices;

  public GeoWaveDataStoreComponents(
      final DataStore dataStore,
      final DataStatisticsStore dataStatisticsStore,
      final AdapterIndexMappingStore indexMappingStore,
      final IndexStore indexStore,
      final InternalGeotoolsFeatureDataAdapter adapter,
      final GeoWaveGTDataStore gtStore,
      final TransactionsAllocator transactionAllocator) {
    this.adapter = adapter;
    this.dataStore = dataStore;
    this.indexStore = indexStore;
    this.dataStatisticsStore = dataStatisticsStore;
    this.indexMappingStore = indexMappingStore;
    this.gtStore = gtStore;
    this.adapterIndices = getPreferredIndices();
    CoordinateReferenceSystem adapterCRS = adapter.getFeatureType().getCoordinateReferenceSystem();
    if (adapterCRS == null) {
      adapterCRS = GeometryUtils.getDefaultCRS();
    }
    if (crs.equals(adapterCRS)) {
      this.featureType = SimpleFeatureTypeBuilder.retype(adapter.getFeatureType(), adapterCRS);
    } else {
      this.featureType = SimpleFeatureTypeBuilder.retype(adapter.getFeatureType(), crs);
    }
    this.gtStore.setPreferredIndices(adapter, adapterIndices);
    this.transactionAllocator = transactionAllocator;
  }

  private Index[] getPreferredIndices() {
    // For now just pick indices that match the CRS of the first spatial index we find
    final AdapterToIndexMapping[] indexMappings =
        indexMappingStore.getIndicesForAdapter(adapter.getAdapterId());
    Index[] preferredIndices = null;
    if ((indexMappings != null) && indexMappings.length > 0) {
      preferredIndices =
          Arrays.stream(indexMappings).map(mapping -> mapping.getIndex(indexStore)).filter(
              index -> {
                final CoordinateReferenceSystem indexCRS;
                if (index.getIndexModel() instanceof CustomCrsIndexModel) {
                  indexCRS = ((CustomCrsIndexModel) index.getIndexModel()).getCrs();
                } else if (SpatialIndexUtils.hasSpatialDimensions(index)) {
                  indexCRS = GeometryUtils.getDefaultCRS();
                } else {
                  return false;
                }
                if (crs == null) {
                  crs = indexCRS;
                } else if (!crs.equals(indexCRS)) {
                  return false;
                }
                return true;
              }).toArray(Index[]::new);
    }
    if (preferredIndices == null || preferredIndices.length == 0) {
      preferredIndices = gtStore.getPreferredIndices(adapter);
      this.crs = GeometryUtils.getIndexCrs(preferredIndices[0]);
    }

    return preferredIndices;
  }

  @SuppressWarnings("unchecked")
  public void initForWrite() {
    // this is ensuring the adapter is properly initialized with the
    // indices and writing it to the adapterStore, in cases where the
    // featuredataadapter was created from geotools datastore's createSchema
    dataStore.addType(adapter, adapterIndices);
  }

  public CoordinateReferenceSystem getCRS() {
    return crs;
  }

  public SimpleFeatureType getFeatureType() {
    return featureType;
  }

  public IndexStore getIndexStore() {
    return indexStore;
  }

  public InternalGeotoolsFeatureDataAdapter getAdapter() {
    return adapter;
  }

  public DataStore getDataStore() {
    return dataStore;
  }

  public AdapterIndexMappingStore getAdapterIndexMappingStore() {
    return indexMappingStore;
  }

  public GeoWaveGTDataStore getGTstore() {
    return gtStore;
  }

  public Index[] getAdapterIndices() {
    return adapterIndices;
  }

  public DataStatisticsStore getStatsStore() {
    return dataStatisticsStore;
  }

  public CloseableIterator<Index> getIndices(
      final StatisticsCache statisticsCache,
      final BasicQueryByClass query,
      final boolean spatialOnly) {
    final GeoWaveGTDataStore gtStore = getGTstore();
    final Map<QueryHint, Object> queryHints = Maps.newHashMap();
    queryHints.put(
        QueryHint.MAX_RANGE_DECOMPOSITION,
        gtStore.getDataStoreOptions().getMaxRangeDecomposition());
    final Index[] indices = gtStore.getIndicesForAdapter(adapter, spatialOnly);
    if (spatialOnly && (indices.length == 0)) {
      throw new UnsupportedOperationException("Query required spatial index, but none were found.");
    }
    return gtStore.getIndexQueryStrategy().getIndices(
        dataStatisticsStore,
        indexMappingStore,
        query,
        indices,
        adapter,
        queryHints);
  }

  public void remove(final SimpleFeature feature, final GeoWaveTransaction transaction)
      throws IOException {
    final VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();

    dataStore.delete(
        bldr.setAuthorizations(transaction.composeAuthorizations()).addTypeName(
            adapter.getTypeName()).constraints(
                bldr.constraintsFactory().dataIds(adapter.getDataId(feature))).build());
  }

  public void remove(final String fid, final GeoWaveTransaction transaction) throws IOException {

    final VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();

    dataStore.delete(
        bldr.setAuthorizations(transaction.composeAuthorizations()).addTypeName(
            adapter.getTypeName()).constraints(
                bldr.constraintsFactory().dataIds(StringUtils.stringToBinary(fid))).build());
  }

  @SuppressWarnings("unchecked")
  public void write(
      final Iterator<SimpleFeature> featureIt,
      final Set<String> fidList,
      final GeoWaveTransaction transaction) throws IOException {
    final VisibilityHandler visibilityHandler =
        new GlobalVisibilityHandler(transaction.composeVisibility());
    dataStore.addType(adapter, adapterIndices);
    try (Writer<SimpleFeature> indexWriter = dataStore.createWriter(adapter.getTypeName())) {
      while (featureIt.hasNext()) {
        final SimpleFeature feature = featureIt.next();
        fidList.add(feature.getID());
        indexWriter.write(feature, visibilityHandler);
      }
    }
  }

  public void writeCommit(final SimpleFeature feature, final GeoWaveTransaction transaction)
      throws IOException {

    final VisibilityHandler visibilityHandler =
        new GlobalVisibilityHandler(transaction.composeVisibility());
    dataStore.addType(adapter, adapterIndices);
    try (Writer<SimpleFeature> indexWriter = dataStore.createWriter(adapter.getTypeName())) {
      indexWriter.write(feature, visibilityHandler);
    }
  }

  public String getTransaction() throws IOException {
    return transactionAllocator.getTransaction();
  }

  public void releaseTransaction(final String txID) throws IOException {
    transactionAllocator.releaseTransaction(txID);
  }
}
