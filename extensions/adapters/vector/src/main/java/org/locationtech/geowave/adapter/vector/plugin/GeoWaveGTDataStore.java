/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.geotools.data.FeatureListenerManager;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.locationtech.geowave.adapter.auth.AuthorizationSPI;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.index.IndexQueryStrategySPI;
import org.locationtech.geowave.adapter.vector.index.SimpleFeaturePrimaryIndexConfiguration;
import org.locationtech.geowave.adapter.vector.plugin.lock.LockingManagement;
import org.locationtech.geowave.adapter.vector.plugin.transaction.GeoWaveAutoCommitTransactionState;
import org.locationtech.geowave.adapter.vector.plugin.transaction.GeoWaveTransactionManagementState;
import org.locationtech.geowave.adapter.vector.plugin.transaction.GeoWaveTransactionState;
import org.locationtech.geowave.adapter.vector.plugin.transaction.MemoryTransactionsAllocator;
import org.locationtech.geowave.adapter.vector.plugin.transaction.TransactionsAllocator;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.InternalGeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.SpatialIndexUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;

public class GeoWaveGTDataStore extends ContentDataStore {
  /** Package logger */
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGTDataStore.class);

  private FeatureListenerManager listenerManager = null;
  protected PersistentAdapterStore adapterStore;
  protected InternalAdapterStore internalAdapterStore;
  protected IndexStore indexStore;
  protected DataStatisticsStore dataStatisticsStore;
  protected DataStore dataStore;
  protected DataStoreOptions dataStoreOptions;
  protected AdapterIndexMappingStore adapterIndexMappingStore;
  private final Map<String, Index[]> preferredIndexes = new ConcurrentHashMap<>();

  private final AuthorizationSPI authorizationSPI;
  private final IndexQueryStrategySPI indexQueryStrategy;
  private final URI featureNameSpaceURI;
  private int transactionBufferSize = 10000;
  private final TransactionsAllocator transactionsAllocator;

  public GeoWaveGTDataStore(final GeoWavePluginConfig config) throws IOException {
    listenerManager = new FeatureListenerManager();
    lockingManager = config.getLockingManagementFactory().createLockingManager(config);
    authorizationSPI = config.getAuthorizationFactory().create(config.getAuthorizationURL());
    init(config);
    featureNameSpaceURI = config.getFeatureNamespace();
    indexQueryStrategy = config.getIndexQueryStrategy();
    transactionBufferSize = config.getTransactionBufferSize();
    transactionsAllocator = new MemoryTransactionsAllocator();
  }

  private void init(final GeoWavePluginConfig config) {
    dataStore = config.getDataStore();
    dataStoreOptions = config.getDataStoreOptions();
    dataStatisticsStore = config.getDataStatisticsStore();
    indexStore = config.getIndexStore();
    adapterStore = config.getAdapterStore();
    adapterIndexMappingStore = config.getAdapterIndexMappingStore();
    internalAdapterStore = config.getInternalAdapterStore();
  }

  public AuthorizationSPI getAuthorizationSPI() {
    return authorizationSPI;
  }

  public FeatureListenerManager getListenerManager() {
    return listenerManager;
  }

  public IndexQueryStrategySPI getIndexQueryStrategy() {
    return indexQueryStrategy;
  }

  public DataStore getDataStore() {
    return dataStore;
  }

  public DataStoreOptions getDataStoreOptions() {
    return dataStoreOptions;
  }

  public PersistentAdapterStore getAdapterStore() {
    return adapterStore;
  }

  public InternalAdapterStore getInternalAdapterStore() {
    return internalAdapterStore;
  }

  public AdapterIndexMappingStore getAdapterIndexMappingStore() {
    return adapterIndexMappingStore;
  }

  public IndexStore getIndexStore() {
    return indexStore;
  }

  public DataStatisticsStore getDataStatisticsStore() {
    return dataStatisticsStore;
  }

  private Index[] filterIndices(final Index[] unfiltered, final boolean spatialOnly) {
    if (spatialOnly) {
      final List<Index> filtered = Lists.newArrayList();
      for (int i = 0; i < unfiltered.length; i++) {
        if (SpatialIndexUtils.hasSpatialDimensions(unfiltered[i])) {
          filtered.add(unfiltered[i]);
        }
      }
      return filtered.toArray(new Index[filtered.size()]);
    }
    return unfiltered;
  }

  public void setPreferredIndices(final GeotoolsFeatureDataAdapter adapter, final Index[] indices) {
    preferredIndexes.put(adapter.getFeatureType().getName().toString(), indices);
  }

  protected Index[] getIndicesForAdapter(
      final GeotoolsFeatureDataAdapter adapter,
      final boolean spatialOnly) {
    Index[] currentSelections = preferredIndexes.get(adapter.getFeatureType().getName().toString());
    if (currentSelections != null) {
      return filterIndices(currentSelections, spatialOnly);
    }

    final short internalAdapterId = internalAdapterStore.getAdapterId(adapter.getTypeName());

    final AdapterToIndexMapping[] adapterIndexMappings =
        adapterIndexMappingStore.getIndicesForAdapter(internalAdapterId);
    if ((adapterIndexMappings != null) && (adapterIndexMappings.length > 0)) {
      currentSelections =
          Arrays.stream(adapterIndexMappings).map(mapping -> mapping.getIndex(indexStore)).toArray(
              Index[]::new);
    } else {
      currentSelections = getPreferredIndices(adapter);
    }
    preferredIndexes.put(adapter.getFeatureType().getName().toString(), currentSelections);
    return filterIndices(currentSelections, spatialOnly);
  }

  @Override
  public void createSchema(final SimpleFeatureType featureType) {
    if (featureType.getGeometryDescriptor() == null) {
      throw new UnsupportedOperationException("Schema missing geometry");
    }
    final FeatureDataAdapter adapter = new FeatureDataAdapter(featureType);
    final short adapterId = internalAdapterStore.addTypeName(adapter.getTypeName());
    if (!adapterStore.adapterExists(adapterId)) {
      if (featureNameSpaceURI != null) {
        adapter.setNamespace(featureNameSpaceURI.toString());
      }
      dataStore.addType(adapter);
    }
  }

  private InternalGeotoolsFeatureDataAdapter getAdapter(final String typeName) {
    final InternalGeotoolsFeatureDataAdapter featureAdapter;
    final Short adapterId = internalAdapterStore.getAdapterId(typeName);
    if (adapterId == null) {
      return null;
    }
    final InternalDataAdapter<?> adapter = adapterStore.getAdapter(adapterId);
    if ((adapter == null) || !(adapter instanceof InternalGeotoolsFeatureDataAdapter)) {
      return null;
    }
    featureAdapter = (InternalGeotoolsFeatureDataAdapter) adapter;
    if (featureNameSpaceURI != null) {
      featureAdapter.setNamespace(featureNameSpaceURI.toString());
    }
    return featureAdapter;
  }

  @Override
  protected List<Name> createTypeNames() throws IOException {
    final List<Name> names = new ArrayList<>();
    final InternalDataAdapter<?>[] adapters = adapterStore.getAdapters();
    for (final InternalDataAdapter<?> adapter : adapters) {
      if (adapter.getAdapter() instanceof GeotoolsFeatureDataAdapter) {
        names.add(((GeotoolsFeatureDataAdapter) adapter.getAdapter()).getFeatureType().getName());
      }
    }
    return names;
  }

  @Override
  public ContentFeatureSource getFeatureSource(final String typeName) throws IOException {
    return getFeatureSource(typeName, Transaction.AUTO_COMMIT);
  }

  @Override
  public ContentFeatureSource getFeatureSource(final String typeName, final Transaction tx)
      throws IOException {
    return super.getFeatureSource(new NameImpl(null, typeName), tx);
  }

  @Override
  public ContentFeatureSource getFeatureSource(final Name typeName, final Transaction tx)
      throws IOException {
    return getFeatureSource(typeName.getLocalPart(), tx);
  }

  @Override
  public ContentFeatureSource getFeatureSource(final Name typeName) throws IOException {
    return getFeatureSource(typeName.getLocalPart(), Transaction.AUTO_COMMIT);
  }

  @Override
  public void dispose() {
    if (dataStore instanceof Closeable) {
      try {
        ((Closeable) dataStore).close();
      } catch (final IOException e) {
        LOGGER.error("Unable to close geowave datastore", e);
      }
    }
  }

  @Override
  protected ContentFeatureSource createFeatureSource(final ContentEntry entry) throws IOException {
    return new GeoWaveFeatureSource(
        entry,
        Query.ALL,
        getAdapter(entry.getTypeName()),
        transactionsAllocator);
  }

  @Override
  public void removeSchema(final Name typeName) throws IOException {
    this.removeSchema(typeName.getLocalPart());
  }

  @Override
  public void removeSchema(final String typeName) throws IOException {
    dataStore.removeType(typeName);
  }

  /**
   * Used to retrieve the TransactionStateDiff for this transaction.
   *
   * <p>
   *
   * @param transaction
   * @return GeoWaveTransactionState or null if subclass is handling differences
   * @throws IOException
   */
  protected GeoWaveTransactionState getMyTransactionState(
      final Transaction transaction,
      final GeoWaveFeatureSource source) throws IOException {
    synchronized (transaction) {
      GeoWaveTransactionState state = null;
      if (transaction == Transaction.AUTO_COMMIT) {
        state = new GeoWaveAutoCommitTransactionState(source);
      } else {
        state = (GeoWaveTransactionState) transaction.getState(this);
        if (state == null) {
          state =
              new GeoWaveTransactionManagementState(
                  transactionBufferSize,
                  source.getComponents(),
                  transaction,
                  (LockingManagement) lockingManager);
          transaction.putState(this, state);
        }
      }
      return state;
    }
  }

  public Index[] getPreferredIndices(final GeotoolsFeatureDataAdapter adapter) {

    final List<Index> currentSelectionsList = new ArrayList<>(2);
    final List<String> indexNames =
        SimpleFeaturePrimaryIndexConfiguration.getIndexNames(adapter.getFeatureType());
    final boolean canUseTime = adapter.hasTemporalConstraints();

    /**
     * Requires the indices to EXIST prior to set up of the adapter. Otherwise, only Geospatial is
     * chosen and the index Names are ignored.
     */
    CoordinateReferenceSystem selectedCRS = null;
    try (CloseableIterator<Index> indices = indexStore.getIndices()) {
      while (indices.hasNext()) {
        final Index index = indices.next();
        final CoordinateReferenceSystem indexCRS = GeometryUtils.getIndexCrs(index);
        if ((selectedCRS != null) && !selectedCRS.equals(indexCRS)) {
          continue;
        }
        if (!indexNames.isEmpty()) {
          // Only used selected preferred indices
          if (indexNames.contains(index.getName())) {
            selectedCRS = indexCRS;
            currentSelectionsList.add(index);
          }
        }

        final NumericDimensionField<?>[] dims = index.getIndexModel().getDimensions();
        boolean hasLat = false;
        boolean hasLong = false;
        boolean hasTime = false;
        for (final NumericDimensionField<?> dim : dims) {
          hasLat |= SpatialIndexUtils.isLatitudeDimension(dim);
          hasLong |= SpatialIndexUtils.isLongitudeDimension(dim);
          hasTime |= dim instanceof TimeField;
        }

        if (hasLat && hasLong) {
          // If not requiring time OR (requires time AND has time
          // constraints)
          if (!hasTime || canUseTime) {
            selectedCRS = indexCRS;
            currentSelectionsList.add(index);
          }
        }
      }
    }

    if (currentSelectionsList.isEmpty()) {
      currentSelectionsList.add(
          SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions()));
    }

    return currentSelectionsList.toArray(new Index[currentSelectionsList.size()]);
  }
}
