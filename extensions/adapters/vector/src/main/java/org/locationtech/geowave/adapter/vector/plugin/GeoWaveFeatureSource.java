/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin;

import java.io.IOException;
import java.util.Map;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureStore;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.geowave.adapter.vector.plugin.transaction.GeoWaveEmptyTransaction;
import org.locationtech.geowave.adapter.vector.plugin.transaction.GeoWaveTransactionState;
import org.locationtech.geowave.adapter.vector.plugin.transaction.TransactionsAllocator;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.api.VectorStatisticsQueryBuilder;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.opengis.feature.FeatureVisitor;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.geometry.BoundingBox;
import org.opengis.util.ProgressListener;

@SuppressWarnings("unchecked")
public class GeoWaveFeatureSource extends ContentFeatureStore {
  private final GeoWaveDataStoreComponents components;

  public GeoWaveFeatureSource(
      final ContentEntry entry,
      final Query query,
      final GeotoolsFeatureDataAdapter adapter,
      final TransactionsAllocator transactionAllocator) {
    super(entry, query);
    components =
        new GeoWaveDataStoreComponents(
            getDataStore().getDataStore(),
            getDataStore().getDataStatisticsStore(),
            getDataStore().getIndexStore(),
            adapter,
            getDataStore(),
            transactionAllocator);
  }

  public GeoWaveDataStoreComponents getComponents() {
    return components;
  }

  @SuppressWarnings("rawtypes")
  @Override
  protected ReferencedEnvelope getBoundsInternal(final Query query) throws IOException {
    double minx = -90.0, maxx = 90.0, miny = -180.0, maxy = 180.0;

    InternalDataStatistics<SimpleFeature, ?, ?> bboxStats = null;
    if (query.getFilter().equals(Filter.INCLUDE)) {
      final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> stats =
          new GeoWaveEmptyTransaction(components).getDataStatistics();
      bboxStats =
          stats.get(
              VectorStatisticsQueryBuilder.newBuilder().factory().bbox().fieldName(
                  getFeatureType().getGeometryDescriptor().getLocalName()).build().getId());
    }
    if (bboxStats != null) {
      minx = ((BoundingBoxDataStatistics) bboxStats).getMinX();
      maxx = ((BoundingBoxDataStatistics) bboxStats).getMaxX();
      miny = ((BoundingBoxDataStatistics) bboxStats).getMinY();
      maxy = ((BoundingBoxDataStatistics) bboxStats).getMaxY();
    } else {

      final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
          new GeoWaveFeatureReader(query, new GeoWaveEmptyTransaction(components), components);
      if (reader.hasNext()) {
        minx = 90.0;
        maxx = -90.0;
        miny = 180.0;
        maxy = -180.0;
        while (reader.hasNext()) {
          final BoundingBox bbox = reader.next().getBounds();
          minx = Math.min(bbox.getMinX(), minx);
          maxx = Math.max(bbox.getMaxX(), maxx);
          miny = Math.min(bbox.getMinY(), miny);
          maxy = Math.max(bbox.getMaxY(), maxy);
        }
      }
      reader.close();
    }
    return new ReferencedEnvelope(minx, maxx, miny, maxy, GeometryUtils.getDefaultCRS());
  }

  @SuppressWarnings("rawtypes")
  @Override
  protected int getCountInternal(final Query query) throws IOException {
    final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> stats =
        new GeoWaveEmptyTransaction(components).getDataStatistics();
    final InternalDataStatistics<SimpleFeature, ?, ?> countStats =
        stats.get(VectorStatisticsQueryBuilder.newBuilder().factory().count().build().getId());
    if ((countStats != null) && query.getFilter().equals(Filter.INCLUDE)) {
      return (int) ((CountDataStatistics) countStats).getCount();
    } else {
      try (GeoWaveFeatureReader reader =
          new GeoWaveFeatureReader(query, new GeoWaveEmptyTransaction(components), components)) {
        return (int) reader.getCount();
      }
    }
  }

  public SimpleFeatureType getFeatureType() {
    return components.getAdapter().getFeatureType();
  }

  @Override
  protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(final Query query)
      throws IOException {
    final GeoWaveTransactionState state = getDataStore().getMyTransactionState(transaction, this);
    return new GeoWaveFeatureReader(
        query,
        state.getGeoWaveTransaction(query.getTypeName()),
        components);
  }

  @Override
  protected FeatureWriter<SimpleFeatureType, SimpleFeature> getWriterInternal(
      final Query query,
      final int flags) throws IOException {
    final GeoWaveTransactionState state = getDataStore().getMyTransactionState(transaction, this);
    return new GeoWaveFeatureWriter(
        components,
        state.getGeoWaveTransaction(query.getTypeName()),
        (GeoWaveFeatureReader) getReaderInternal(query));
  }

  @Override
  public void accepts(
      final Query query,
      final FeatureVisitor visitor,
      final ProgressListener progress) throws IOException {
    if (!GeoWaveGTPluginUtils.accepts(
        visitor,
        progress,
        getFeatureType(),
        getDataStore().getMyTransactionState(transaction, this).getGeoWaveTransaction(
            getFeatureType().getTypeName()).getDataStatistics())) {
      super.accepts(query, visitor, progress);
    }
  }

  @Override
  protected SimpleFeatureType buildFeatureType() throws IOException {
    return components.getAdapter().getFeatureType();
  }

  @Override
  public GeoWaveGTDataStore getDataStore() {
    // type narrow this method to prevent a lot of casts resulting in more
    // readable code.
    return (GeoWaveGTDataStore) super.getDataStore();
  }

  @Override
  protected boolean canTransact() {
    // tell GeoTools that we natively handle this
    return true;
  }

  @Override
  protected boolean canLock() {
    // tell GeoTools that we natively handle this
    return true;
  }

  @Override
  protected boolean canFilter() {
    return true;
  }

  @Override
  protected void doLockInternal(final String typeName, final SimpleFeature feature)
      throws IOException {
    getDataStore().getLockingManager().lockFeatureID(typeName, feature.getID(), transaction, lock);
  }

  @Override
  protected void doUnlockInternal(final String typeName, final SimpleFeature feature)
      throws IOException {
    getDataStore().getLockingManager().unLockFeatureID(
        typeName,
        feature.getID(),
        transaction,
        lock);
  }
}
