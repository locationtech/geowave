/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.AbstractAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.MapRowBuilder;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.MultiFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.query.filter.expression.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Accepts entries that pass the given GeoWave filter expression.
 */
public class ExpressionQueryFilter<T> implements QueryFilter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExpressionQueryFilter.class);
  private InternalDataAdapter<T> adapter;
  private AdapterToIndexMapping indexMapping;
  private Filter filter;
  private Set<String> referencedFields = null;
  private Map<String, IndexFieldMapper<?, ?>> fieldToIndexFieldMap = null;
  private boolean referencedFieldsInitialized = false;

  public ExpressionQueryFilter() {
    super();
  }

  public ExpressionQueryFilter(
      final Filter filter,
      final InternalDataAdapter<T> adapter,
      final AdapterToIndexMapping indexMapping) {
    this.filter = filter;
    this.adapter = adapter;
    this.indexMapping = indexMapping;
  }

  public String getTypeName() {
    return adapter.getTypeName();
  }

  public Filter getFilter() {
    return filter;
  }

  private void initReferencedFields() {
    synchronized (indexMapping) {
      if (!referencedFieldsInitialized) {
        this.referencedFields = Sets.newHashSet();
        this.fieldToIndexFieldMap = Maps.newHashMap();
        filter.addReferencedFields(referencedFields);
        for (final IndexFieldMapper<?, ?> mapper : indexMapping.getIndexFieldMappers()) {
          for (final String field : mapper.getAdapterFields()) {
            fieldToIndexFieldMap.put(field, mapper);
          }
        }
        referencedFieldsInitialized = true;
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public boolean accept(
      final CommonIndexModel indexModel,
      final IndexedPersistenceEncoding<?> persistenceEncoding) {
    if ((filter != null) && (indexModel != null) && (adapter != null) && (indexMapping != null)) {
      final Map<String, Object> fieldValues = Maps.newHashMap();
      if (!referencedFieldsInitialized) {
        initReferencedFields();
      }
      final PersistentDataset<?> commonData = persistenceEncoding.getCommonData();
      PersistentDataset<Object> adapterExtendedValues = null;
      for (final String field : referencedFields) {
        if (fieldValues.containsKey(field)) {
          continue;
        }
        if (fieldToIndexFieldMap.containsKey(field)) {
          final IndexFieldMapper<?, ?> mapper = fieldToIndexFieldMap.get(field);
          final Object indexValue = commonData.getValue(mapper.indexFieldName());
          ((IndexFieldMapper) mapper).toAdapter(indexValue, new MapRowBuilder(fieldValues));
        } else {
          final Object value = commonData.getValue(field);
          if (value != null) {
            fieldValues.put(field, value);
          } else {
            if (adapterExtendedValues == null) {
              adapterExtendedValues = new MultiFieldPersistentDataset<>();
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
            }
            fieldValues.put(field, adapterExtendedValues.getValue(field));
          }
        }
      }
      return filter.evaluate(fieldValues);
    }
    return true;
  }

  @Override
  public byte[] toBinary() {
    byte[] filterBytes;
    if (filter == null) {
      LOGGER.warn("Filter is null");
      filterBytes = new byte[] {};
    } else {
      filterBytes = PersistenceUtils.toBinary(filter);
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

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int filterBytesLength = VarintUtils.readUnsignedInt(buf);
    if (filterBytesLength > 0) {
      final byte[] filterBytes = ByteArrayUtils.safeRead(buf, filterBytesLength);
      filter = (Filter) PersistenceUtils.fromBinary(filterBytes);
    } else {
      LOGGER.warn("Filter is empty bytes");
      filter = null;
    }

    final int adapterBytesLength = VarintUtils.readUnsignedInt(buf);
    if (adapterBytesLength > 0) {
      final byte[] adapterBytes = ByteArrayUtils.safeRead(buf, adapterBytesLength);

      try {
        adapter = (InternalDataAdapter) PersistenceUtils.fromBinary(adapterBytes);
      } catch (final Exception e) {
        throw new IllegalArgumentException("Unable to read adapter from binary", e);
      }
    } else {
      LOGGER.warn("Data Adapter is empty bytes");
      adapter = null;
    }

    final int mappingBytesLength = buf.remaining();
    if (mappingBytesLength > 0) {
      final byte[] mappingBytes = ByteArrayUtils.safeRead(buf, mappingBytesLength);

      try {
        indexMapping = (AdapterToIndexMapping) PersistenceUtils.fromBinary(mappingBytes);
      } catch (final Exception e) {
        throw new IllegalArgumentException(
            "Unable to read adapter to index mapping from binary",
            e);
      }
    } else {
      LOGGER.warn("Adapter to index mapping is empty bytes");
      indexMapping = null;
    }
  }
}
