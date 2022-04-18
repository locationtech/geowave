/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter;

import java.nio.ByteBuffer;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geowave.core.geotime.store.InternalGeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.util.FilterToCQLTool;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.AbstractAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.MultiFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.IndexImpl;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CQLQueryFilter implements QueryFilter {
  private static final Logger LOGGER = LoggerFactory.getLogger(CQLQueryFilter.class);
  private InternalGeotoolsFeatureDataAdapter<?> adapter;
  private AdapterToIndexMapping indexMapping;
  private Filter filter;

  public CQLQueryFilter() {
    super();
  }

  public CQLQueryFilter(
      final Filter filter,
      final InternalGeotoolsFeatureDataAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping) {
    this.filter = FilterToCQLTool.fixDWithin(filter);
    this.adapter = adapter;
    this.indexMapping = indexMapping;
  }

  public String getTypeName() {
    return adapter.getTypeName();
  }

  @Override
  public boolean accept(
      final CommonIndexModel indexModel,
      final IndexedPersistenceEncoding persistenceEncoding) {
    if ((filter != null) && (indexModel != null) && (adapter != null)) {
      final PersistentDataset<Object> adapterExtendedValues = new MultiFieldPersistentDataset<>();
      if (persistenceEncoding instanceof AbstractAdapterPersistenceEncoding) {
        ((AbstractAdapterPersistenceEncoding) persistenceEncoding).convertUnknownValues(
            adapter,
            indexModel);
        final PersistentDataset<Object> existingExtValues =
            ((AbstractAdapterPersistenceEncoding) persistenceEncoding).getAdapterExtendedData();

        if (persistenceEncoding.isAsync()) {
          return false;
        }
        if (existingExtValues != null) {
          adapterExtendedValues.addValues(existingExtValues.getValues());
        }
      }
      final IndexedAdapterPersistenceEncoding encoding =
          new IndexedAdapterPersistenceEncoding(
              persistenceEncoding.getInternalAdapterId(),
              persistenceEncoding.getDataId(),
              persistenceEncoding.getInsertionPartitionKey(),
              persistenceEncoding.getInsertionSortKey(),
              persistenceEncoding.getDuplicateCount(),
              persistenceEncoding.getCommonData(),
              new MultiFieldPersistentDataset<byte[]>(),
              adapterExtendedValues);

      final SimpleFeature feature =
          (SimpleFeature) adapter.decode(
              encoding,
              indexMapping,
              new IndexImpl(
                  null, // because we
                  // know the
                  // feature data
                  // adapter doesn't use the numeric
                  // index
                  // strategy and only the common
                  // index
                  // model to decode the simple
                  // feature,
                  // we pass along a null strategy to
                  // eliminate the necessity to send a
                  // serialization of the strategy in
                  // the
                  // options of this iterator
                  indexModel));
      if (feature == null) {
        return false;
      }
      return filter.evaluate(feature);
    }
    return true;
  }

  @Override
  public byte[] toBinary() {
    byte[] filterBytes;
    if (filter == null) {
      LOGGER.warn("CQL filter is null");
      filterBytes = new byte[] {};
    } else {
      filterBytes = StringUtils.stringToBinary(ECQL.toCQL(filter));
    }
    byte[] adapterBytes;
    if (adapter != null) {
      adapterBytes = PersistenceUtils.toBinary(adapter);
    } else {
      LOGGER.warn("Feature Data Adapter is null");
      adapterBytes = new byte[] {};
    }
    byte[] mappingBytes;
    if (indexMapping != null) {
      mappingBytes = PersistenceUtils.toBinary(indexMapping);
    } else {
      LOGGER.warn("Adapter to index mapping is null");
      mappingBytes = new byte[] {};
    }
    final ByteBuffer buf =
        ByteBuffer.allocate(
            filterBytes.length
                + adapterBytes.length
                + mappingBytes.length
                + VarintUtils.unsignedIntByteLength(filterBytes.length)
                + VarintUtils.unsignedIntByteLength(adapterBytes.length));
    VarintUtils.writeUnsignedInt(filterBytes.length, buf);
    buf.put(filterBytes);
    VarintUtils.writeUnsignedInt(adapterBytes.length, buf);
    buf.put(adapterBytes);
    buf.put(mappingBytes);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    GeometryUtils.initClassLoader();
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int filterBytesLength = VarintUtils.readUnsignedInt(buf);
    if (filterBytesLength > 0) {
      final byte[] filterBytes = ByteArrayUtils.safeRead(buf, filterBytesLength);
      final String cql = StringUtils.stringFromBinary(filterBytes);
      try {
        filter = ECQL.toFilter(cql);
      } catch (final Exception e) {
        throw new IllegalArgumentException(cql, e);
      }
    } else {
      LOGGER.warn("CQL filter is empty bytes");
      filter = null;
    }

    final int adapterBytesLength = VarintUtils.readUnsignedInt(buf);
    if (adapterBytesLength > 0) {
      final byte[] adapterBytes = ByteArrayUtils.safeRead(buf, adapterBytesLength);

      try {
        adapter = (InternalGeotoolsFeatureDataAdapter<?>) PersistenceUtils.fromBinary(adapterBytes);
      } catch (final Exception e) {
        throw new IllegalArgumentException("Unable to read adapter from CQL filter binary", e);
      }
    } else {
      LOGGER.warn("Feature Data Adapter is empty bytes");
      adapter = null;
    }

    final int mappingBytesLength = buf.remaining();
    if (adapterBytesLength > 0) {
      final byte[] mappingBytes = ByteArrayUtils.safeRead(buf, mappingBytesLength);

      try {
        indexMapping = (AdapterToIndexMapping) PersistenceUtils.fromBinary(mappingBytes);
      } catch (final Exception e) {
        throw new IllegalArgumentException(
            "Unable to read adapter to index mapping from CQL filter binary",
            e);
      }
    } else {
      LOGGER.warn("Adapter to index mapping is empty bytes");
      indexMapping = null;
    }
  }
}
