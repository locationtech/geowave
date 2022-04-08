/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.ingest;

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

/*
 */
public abstract class MinimalSimpleFeatureIngestPlugin implements
    LocalFileIngestPlugin<SimpleFeature>,
    Persistable {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MinimalSimpleFeatureIngestPlugin.class);
  protected CQLFilterOptionProvider filterOptionProvider = new CQLFilterOptionProvider();
  protected TypeNameOptionProvider typeNameProvider = new TypeNameOptionProvider();
  protected GeometrySimpOptionProvider simpOptionProvider = new GeometrySimpOptionProvider();

  public void setFilterProvider(final CQLFilterOptionProvider filterOptionProvider) {
    this.filterOptionProvider = filterOptionProvider;
  }

  public void setTypeNameProvider(final TypeNameOptionProvider typeNameProvider) {
    this.typeNameProvider = typeNameProvider;
  }

  public void setGeometrySimpOptionProvider(final GeometrySimpOptionProvider geometryProvider) {
    this.simpOptionProvider = geometryProvider;
  }

  @Override
  public byte[] toBinary() {
    final byte[] filterBinary = filterOptionProvider.toBinary();
    final byte[] typeNameBinary = typeNameProvider.toBinary();
    final byte[] simpBinary = simpOptionProvider.toBinary();
    final ByteBuffer buf =
        ByteBuffer.allocate(
            filterBinary.length
                + typeNameBinary.length
                + simpBinary.length
                + VarintUtils.unsignedIntByteLength(filterBinary.length)
                + VarintUtils.unsignedIntByteLength(typeNameBinary.length)
                + VarintUtils.unsignedIntByteLength(simpBinary.length));
    VarintUtils.writeUnsignedInt(filterBinary.length, buf);
    buf.put(filterBinary);
    VarintUtils.writeUnsignedInt(typeNameBinary.length, buf);
    buf.put(typeNameBinary);
    VarintUtils.writeUnsignedInt(simpBinary.length, buf);
    buf.put(simpBinary);

    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int filterBinaryLength = VarintUtils.readUnsignedInt(buf);
    final byte[] filterBinary = ByteArrayUtils.safeRead(buf, filterBinaryLength);

    final int typeNameBinaryLength = VarintUtils.readUnsignedInt(buf);
    final byte[] typeNameBinary = ByteArrayUtils.safeRead(buf, typeNameBinaryLength);

    final int geometrySimpLength = VarintUtils.readUnsignedInt(buf);
    final byte[] geometrySimpBinary = ByteArrayUtils.safeRead(buf, geometrySimpLength);

    filterOptionProvider = new CQLFilterOptionProvider();
    filterOptionProvider.fromBinary(filterBinary);

    typeNameProvider = new TypeNameOptionProvider();
    typeNameProvider.fromBinary(typeNameBinary);

    simpOptionProvider = new GeometrySimpOptionProvider();
    simpOptionProvider.fromBinary(geometrySimpBinary);
  }

  @Override
  public String[] getFileExtensionFilters() {
    return new String[0];
  }

  @Override
  public void init(URL url) {}


  @Override
  public Index[] getRequiredIndices() {
    return new Index[] {};
  }

  @Override
  public String[] getSupportedIndexTypes() {
    return new String[] {SpatialField.DEFAULT_GEOMETRY_FIELD_NAME, TimeField.DEFAULT_FIELD_ID};
  }

  protected DataTypeAdapter<SimpleFeature> newAdapter(final SimpleFeatureType type) {
    return new FeatureDataAdapter(type);
  }

  protected abstract SimpleFeatureType[] getTypes();

  protected abstract CloseableIterator<SimpleFeature> getFeatures(URL input);

  @Override
  public DataTypeAdapter<SimpleFeature>[] getDataAdapters() {
    final SimpleFeatureType[] types = getTypes();
    final DataTypeAdapter<SimpleFeature>[] retVal = new FeatureDataAdapter[types.length];
    for (int i = 0; i < types.length; i++) {
      retVal[i] = newAdapter(types[i]);
    }
    return retVal;
  }

  @Override
  public CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveData(
      final URL input,
      final String[] indexNames) {
    final CloseableIterator<SimpleFeature> filteredFeatures = applyFilters(getFeatures(input));
    return toGeoWaveDataInternal(filteredFeatures, indexNames);
  }

  private CloseableIterator<SimpleFeature> applyFilters(
      final CloseableIterator<SimpleFeature> source) {
    final CQLFilterOptionProvider internalFilterProvider;
    if ((filterOptionProvider != null)
        && (filterOptionProvider.getCqlFilterString() != null)
        && !filterOptionProvider.getCqlFilterString().trim().isEmpty()) {
      internalFilterProvider = filterOptionProvider;
    } else {
      internalFilterProvider = null;
    }
    final TypeNameOptionProvider internalTypeNameProvider;
    if ((typeNameProvider != null)
        && (typeNameProvider.getTypeName() != null)
        && !typeNameProvider.getTypeName().trim().isEmpty()) {
      internalTypeNameProvider = typeNameProvider;
    } else {
      internalTypeNameProvider = null;
    }
    final GeometrySimpOptionProvider internalSimpOptionProvider;
    if ((simpOptionProvider != null)) {
      internalSimpOptionProvider = simpOptionProvider;
    } else {
      internalSimpOptionProvider = null;
    }
    if ((internalFilterProvider != null) || (internalTypeNameProvider != null)) {
      final Iterator<SimpleFeature> it = Iterators.filter(source, new Predicate<SimpleFeature>() {
        @Override
        public boolean apply(final SimpleFeature input) {
          if ((internalTypeNameProvider != null)
              && !internalTypeNameProvider.typeNameMatches(input.getFeatureType().getTypeName())) {
            return false;
          }
          if ((internalFilterProvider != null) && !internalFilterProvider.evaluate(input)) {
            return false;
          }
          if ((internalSimpOptionProvider != null)) {
            final Geometry simpGeom =
                internalSimpOptionProvider.simplifyGeometry((Geometry) input.getDefaultGeometry());
            if (!internalSimpOptionProvider.filterGeometry(simpGeom)) {
              return false;
            }
            input.setDefaultGeometry(simpGeom);
          }
          return true;
        }
      });
      return new CloseableIteratorWrapper<>(source, it);
    }
    return source;
  }

  private CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
      final CloseableIterator<SimpleFeature> source,
      final String[] indexNames) {
    final Iterator<GeoWaveData<SimpleFeature>> geowaveData =
        Iterators.transform(source, feature -> {
          return new GeoWaveData<>(feature.getFeatureType().getTypeName(), indexNames, feature);
        });
    return new CloseableIteratorWrapper<>(source, geowaveData);
  }
}
