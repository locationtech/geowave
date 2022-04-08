/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.data.MultiFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Maps;

/**
 * This class generically supports most of the operations necessary to implement a Data Adapter and
 * can be easily extended to support specific data types.<br> Many of the details are handled by
 * mapping IndexFieldHandler's based on either types or exact dimensions. These handler mappings can
 * be supplied in the constructor. The dimension matching handlers are used first when trying to
 * decode a persistence encoded value. This can be done specifically to match a field (for example
 * if there are multiple ways of encoding/decoding the same type). Otherwise the type matching
 * handlers will simply match any field with the same type as its generic field type.
 *
 * @param <T> The type for the entries handled by this adapter
 */
public class InternalDataAdapterImpl<T> implements InternalDataAdapter<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(InternalDataAdapterImpl.class);

  // this is not thread-safe, but should be ok given the only modification is on initialization
  // which is a synchronized operation
  /** Map of Field Readers associated with a Field ID */
  private final Map<String, FieldReader<Object>> mapOfFieldNameToReaders = new HashMap<>();
  /** Map of Field Writers associated with a Field ID */
  private final Map<String, FieldWriter<Object>> mapOfFieldNameToWriters = new HashMap<>();
  private transient Map<String, Integer> fieldToPositionMap = null;
  private transient Map<Integer, String> positionToFieldMap = null;
  private transient Map<String, List<String>> modelToDimensionsMap = null;
  private transient volatile boolean positionMapsInitialized = false;
  private Object MUTEX = new Object();
  protected DataTypeAdapter<T> adapter;
  protected short adapterId;
  protected VisibilityHandler visibilityHandler = null;

  public InternalDataAdapterImpl() {}

  public InternalDataAdapterImpl(final DataTypeAdapter<T> adapter, final short adapterId) {
    this(adapter, adapterId, null);
  }

  public InternalDataAdapterImpl(
      final DataTypeAdapter<T> adapter,
      final short adapterId,
      final VisibilityHandler visibilityHandler) {
    this.adapter = adapter;
    this.adapterId = adapterId;
    this.visibilityHandler = visibilityHandler;
  }

  @Override
  public VisibilityHandler getVisibilityHandler() {
    return visibilityHandler;
  }

  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings()
  protected List<String> getDimensionFieldNames(final CommonIndexModel model) {
    if (modelToDimensionsMap == null) {
      synchronized (MUTEX) {
        if (modelToDimensionsMap == null) {
          modelToDimensionsMap = new ConcurrentHashMap<>();
        }
      }
    }
    final List<String> retVal = modelToDimensionsMap.get(model.getId());
    if (retVal != null) {
      return retVal;
    }
    final List<String> dimensionFieldNames = DataStoreUtils.getUniqueDimensionFields(model);
    modelToDimensionsMap.put(model.getId(), dimensionFieldNames);
    return dimensionFieldNames;
  }

  @Override
  public AdapterPersistenceEncoding encode(
      final T entry,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    final PersistentDataset<Object> indexData = new MultiFieldPersistentDataset<>();
    final Set<String> nativeFieldsInIndex = new HashSet<>();
    final Set<String> dimensionFieldsUsed = new HashSet<>();
    if (indexMapping != null) {
      for (final IndexFieldMapper<?, ?> indexField : indexMapping.getIndexFieldMappers()) {
        if (dimensionFieldsUsed.add(indexField.indexFieldName())) {
          final Object value = InternalAdapterUtils.entryToIndexValue(indexField, adapter, entry);
          if (value == null) {
            // The field value cannot be mapped to the index (such as null field values)
            return null;
          }
          indexData.addValue(indexField.indexFieldName(), value);
          Collections.addAll(nativeFieldsInIndex, indexField.getAdapterFields());
        }
      }
    }

    final PersistentDataset<Object> extendedData = new MultiFieldPersistentDataset<>();

    // now for the other data
    for (final FieldDescriptor<?> desc : adapter.getFieldDescriptors()) {
      final String fieldName = desc.fieldName();
      if (nativeFieldsInIndex.contains(fieldName)) {
        continue;
      }
      extendedData.addValue(fieldName, adapter.getFieldValue(entry, fieldName));
    }

    return new AdapterPersistenceEncoding(adapterId, getDataId(entry), indexData, extendedData);
  }

  @Override
  public InternalDataAdapter<T> asInternalAdapter(final short internalAdapterId) {
    return adapter.asInternalAdapter(internalAdapterId);
  }

  @Override
  public InternalDataAdapter<T> asInternalAdapter(
      final short internalAdapterId,
      final VisibilityHandler visibilityHandler) {
    return adapter.asInternalAdapter(internalAdapterId, visibilityHandler);
  }

  @Override
  public boolean isCommonIndexField(
      final AdapterToIndexMapping indexMapping,
      final String fieldName) {
    for (final IndexFieldMapper<?, ?> indexField : indexMapping.getIndexFieldMappers()) {
      if (Arrays.stream(indexField.getAdapterFields()).anyMatch(field -> field.equals(fieldName))) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public T decode(
      final IndexedAdapterPersistenceEncoding data,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    final RowBuilder<T> builder = getRowBuilder(indexMapping);
    if (indexMapping != null) {
      for (final IndexFieldMapper<?, ?> fieldMapper : indexMapping.getIndexFieldMappers()) {
        final String fieldName = fieldMapper.indexFieldName();
        final Object value = data.getCommonData().getValue(fieldName);
        if (value == null) {
          continue;
        }
        ((IndexFieldMapper) fieldMapper).toAdapter(value, builder);
      }
    }
    builder.setFields(data.getAdapterExtendedData().getValues());
    return builder.buildRow(data.getDataId());
  }

  @Override
  public byte[] toBinary() {
    final byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
    final byte[] visibilityHanlderBytes = PersistenceUtils.toBinary(visibilityHandler);
    final ByteBuffer buffer =
        ByteBuffer.allocate(
            Short.BYTES
                + VarintUtils.unsignedIntByteLength(adapterBytes.length)
                + adapterBytes.length
                + VarintUtils.unsignedIntByteLength(visibilityHanlderBytes.length)
                + visibilityHanlderBytes.length);
    buffer.putShort(adapterId);
    VarintUtils.writeUnsignedInt(adapterBytes.length, buffer);
    buffer.put(adapterBytes);
    VarintUtils.writeUnsignedInt(visibilityHanlderBytes.length, buffer);
    buffer.put(visibilityHanlderBytes);
    return buffer.array();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void fromBinary(final byte[] bytes) {
    if ((bytes == null) || (bytes.length == 0)) {
      LOGGER.warn("Unable to deserialize data adapter.  Binary is incomplete.");
      return;
    }
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    adapterId = buffer.getShort();
    final byte[] adapterBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(adapterBytes);
    adapter = (DataTypeAdapter<T>) PersistenceUtils.fromBinary(adapterBytes);
    final byte[] visibilityHandlerBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(visibilityHandlerBytes);
    visibilityHandler = (VisibilityHandler) PersistenceUtils.fromBinary(visibilityHandlerBytes);
  }

  @Override
  public FieldReader<Object> getReader(final String fieldName) {
    FieldReader<Object> reader = mapOfFieldNameToReaders.get(fieldName);

    // Check the map to see if a reader has already been found.
    if (reader == null) {
      // Reader not in Map, go to the adapter and get the reader
      reader = adapter.getReader(fieldName);

      // Add it to map for the next time
      mapOfFieldNameToReaders.put(fieldName, reader);
    }

    return reader;
  }

  @Override
  public FieldWriter<Object> getWriter(final String fieldName) {
    // Go to the map to get a writer for given fieldId

    FieldWriter<Object> writer = mapOfFieldNameToWriters.get(fieldName);

    // Check the map to see if a writer has already been found.
    if (writer == null) {
      // Writer not in Map, go to the adapter and get the writer
      writer = adapter.getWriter(fieldName);

      // Add it to map for the next time
      mapOfFieldNameToWriters.put(fieldName, writer);
    }
    return writer;
  }

  @Override
  public String getTypeName() {
    return adapter.getTypeName();
  }

  @Override
  public byte[] getDataId(final T entry) {
    return adapter.getDataId(entry);
  }

  @Override
  public Object getFieldValue(final T entry, final String fieldName) {
    return adapter.getFieldValue(entry, fieldName);
  }

  @Override
  public Class<T> getDataClass() {
    return adapter.getDataClass();
  }

  private ThreadLocal<RowBuilder<T>> builder = null;

  public RowBuilder<T> getRowBuilder(final AdapterToIndexMapping indexMapping) {
    if (builder == null) {
      final FieldDescriptor<?>[] outputFieldDescriptors = adapter.getFieldDescriptors();
      if (indexMapping != null) {
        indexMapping.getIndexFieldMappers().forEach(
            mapping -> mapping.transformFieldDescriptors(outputFieldDescriptors));
      }

      builder = new ThreadLocal<RowBuilder<T>>() {
        @Override
        protected RowBuilder<T> initialValue() {
          return adapter.newRowBuilder(outputFieldDescriptors);
        }
      };
    }
    return builder.get();
  }

  @Override
  public RowBuilder<T> newRowBuilder(final FieldDescriptor<?>[] outputFieldDescriptors) {
    return adapter.newRowBuilder(outputFieldDescriptors);
  }

  @Override
  public FieldDescriptor<?>[] getFieldDescriptors() {
    return adapter.getFieldDescriptors();
  }

  @Override
  public FieldDescriptor<?> getFieldDescriptor(final String fieldName) {
    return adapter.getFieldDescriptor(fieldName);
  }

  @Override
  public short getAdapterId() {
    return adapterId;
  }

  @Override
  public DataTypeAdapter<T> getAdapter() {
    return adapter;
  }

  @Override
  public int getPositionOfOrderedField(final CommonIndexModel model, final String fieldName) {
    int numDimensions;
    if (model != null) {
      final List<String> dimensionFieldNames = getDimensionFieldNames(model);
      // first check CommonIndexModel dimensions
      if (dimensionFieldNames.contains(fieldName)) {
        return dimensionFieldNames.indexOf(fieldName);
      }
      numDimensions = dimensionFieldNames.size();
    } else {
      numDimensions = 0;
    }
    if (!positionMapsInitialized) {
      synchronized (MUTEX) {
        initializePositionMaps();
      }
    }
    // next check other fields
    // dimension fields must be first, add padding
    final Integer position = fieldToPositionMap.get(fieldName);
    if (position == null) {
      return -1;
    }
    return position.intValue() + numDimensions;
  }

  @Override
  public String getFieldNameForPosition(final CommonIndexModel model, final int position) {
    final List<String> dimensionFieldNames = getDimensionFieldNames(model);
    if (position >= dimensionFieldNames.size()) {
      final int adjustedPosition = position - dimensionFieldNames.size();
      if (!positionMapsInitialized) {
        synchronized (MUTEX) {
          initializePositionMaps();
        }
      }
      // check other fields
      return positionToFieldMap.get(adjustedPosition);
    }
    // otherwise check CommonIndexModel dimensions
    return dimensionFieldNames.get(position);
  }

  private void initializePositionMaps() {
    if (positionMapsInitialized) {
      return;
    }
    try {
      fieldToPositionMap = Maps.newHashMap();
      positionToFieldMap = Maps.newHashMap();
      final FieldDescriptor<?>[] fields = adapter.getFieldDescriptors();
      for (int i = 0; i < fields.length; i++) {
        final String currFieldName = fields[i].fieldName();
        fieldToPositionMap.put(currFieldName, i);
        positionToFieldMap.put(i, currFieldName);
      }
      positionMapsInitialized = true;
    } catch (final Exception e) {
      LOGGER.error("Unable to initialize position map, continuing anyways", e);
    }
  }
}
