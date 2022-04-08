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
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureStore;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.geowave.adapter.vector.plugin.transaction.GeoWaveEmptyTransaction;
import org.locationtech.geowave.adapter.vector.plugin.transaction.GeoWaveTransactionState;
import org.locationtech.geowave.adapter.vector.plugin.transaction.StatisticsCache;
import org.locationtech.geowave.adapter.vector.plugin.transaction.TransactionsAllocator;
import org.locationtech.geowave.core.geotime.store.InternalGeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic.BoundingBoxValue;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.opengis.feature.FeatureVisitor;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import org.opengis.util.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoWaveFeatureSource extends ContentFeatureStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveFeatureSource.class);
  private final GeoWaveDataStoreComponents components;

  public GeoWaveFeatureSource(
      final ContentEntry entry,
      final Query query,
      final InternalGeotoolsFeatureDataAdapter adapter,
      final TransactionsAllocator transactionAllocator) {
    super(entry, query);
    components =
        new GeoWaveDataStoreComponents(
            getDataStore().getDataStore(),
            getDataStore().getDataStatisticsStore(),
            getDataStore().getAdapterIndexMappingStore(),
            getDataStore().getIndexStore(),
            adapter,
            getDataStore(),
            transactionAllocator);
  }

  public GeoWaveDataStoreComponents getComponents() {
    return components;
  }

  @Override
  protected ReferencedEnvelope getBoundsInternal(final Query query) throws IOException {
    double minx = -90.0, maxx = 90.0, miny = -180.0, maxy = 180.0;

    BoundingBoxValue bboxStats = null;
    if (query.getFilter().equals(Filter.INCLUDE)) {
      final StatisticsCache statsCache =
          new GeoWaveEmptyTransaction(components).getDataStatistics();
      bboxStats =
          statsCache.getFieldStatistic(
              BoundingBoxStatistic.STATS_TYPE,
              getFeatureType().getGeometryDescriptor().getLocalName());
    }
    CoordinateReferenceSystem bboxCRS = DefaultGeographicCRS.WGS84;
    if (bboxStats != null) {
      minx = bboxStats.getMinX();
      maxx = bboxStats.getMaxX();
      miny = bboxStats.getMinY();
      maxy = bboxStats.getMaxY();
      BoundingBoxStatistic statistic = (BoundingBoxStatistic) bboxStats.getStatistic();
      if (statistic.getDestinationCrs() != null) {
        bboxCRS = statistic.getDestinationCrs();
      } else {
        bboxCRS = components.getAdapter().getFeatureType().getCoordinateReferenceSystem();
      }
    } else {
      final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
          new GeoWaveFeatureReader(query, new GeoWaveEmptyTransaction(components), components);
      if (reader.hasNext()) {
        bboxCRS = components.getCRS();
        BoundingBox featureBounds = reader.next().getBounds();
        minx = featureBounds.getMinX();
        maxx = featureBounds.getMaxX();
        miny = featureBounds.getMinY();
        maxy = featureBounds.getMaxY();
        while (reader.hasNext()) {
          featureBounds = reader.next().getBounds();
          minx = Math.min(featureBounds.getMinX(), minx);
          maxx = Math.max(featureBounds.getMaxX(), maxx);
          miny = Math.min(featureBounds.getMinY(), miny);
          maxy = Math.max(featureBounds.getMaxY(), maxy);
        }
      }
      reader.close();
    }
    ReferencedEnvelope retVal = new ReferencedEnvelope(minx, maxx, miny, maxy, bboxCRS);
    if (!bboxCRS.equals(components.getCRS())) {
      try {
        retVal = retVal.transform(components.getCRS(), true);
      } catch (FactoryException | TransformException e) {
        LOGGER.warn("Unable to transform bounding box for feature source.");
      }
    }
    return retVal;
  }

  @Override
  protected int getCountInternal(final Query query) throws IOException {
    final CountValue count =
        new GeoWaveEmptyTransaction(components).getDataStatistics().getAdapterStatistic(
            CountStatistic.STATS_TYPE);
    if ((count != null) && query.getFilter().equals(Filter.INCLUDE)) {
      return count.getValue().intValue();
    } else {
      try (GeoWaveFeatureReader reader =
          new GeoWaveFeatureReader(query, new GeoWaveEmptyTransaction(components), components)) {
        return (int) reader.getCount();
      }
    }
  }

  public SimpleFeatureType getFeatureType() {
    return components.getFeatureType();
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
        components.getStatsStore(),
        components.getAdapter(),
        visitor,
        progress,
        getFeatureType())) {
      super.accepts(query, visitor, progress);
    }
  }

  @Override
  protected SimpleFeatureType buildFeatureType() throws IOException {
    return getFeatureType();
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
