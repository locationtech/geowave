/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentValue;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.util.GenericTypeResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public abstract class AbstractDataAdapter<T> implements DataTypeAdapter<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDataAdapter.class);
  protected Map<Class<?>, IndexFieldHandler<T, ? extends CommonIndexValue, Object>> typeMatchingFieldHandlers;
  protected Map<String, IndexFieldHandler<T, ? extends CommonIndexValue, Object>> fieldNameMatchingFieldHandlers;
  protected List<NativeFieldHandler<T, Object>> nativeFieldHandlers;
  protected FieldVisibilityHandler<T, Object> fieldVisiblityHandler;

  protected AbstractDataAdapter() {}

  public AbstractDataAdapter(
      final List<PersistentIndexFieldHandler<T, ? extends CommonIndexValue, Object>> indexFieldHandlers,
      final List<NativeFieldHandler<T, Object>> nativeFieldHandlers,
      final FieldVisibilityHandler<T, Object> fieldVisiblityHandler) {
    this(indexFieldHandlers, nativeFieldHandlers, fieldVisiblityHandler, null);
  }

  public AbstractDataAdapter(
      final List<PersistentIndexFieldHandler<T, ? extends CommonIndexValue, Object>> indexFieldHandlers,
      final List<NativeFieldHandler<T, Object>> nativeFieldHandlers) {
    this(indexFieldHandlers, nativeFieldHandlers, null, null);
  }

  protected AbstractDataAdapter(
      final List<PersistentIndexFieldHandler<T, ? extends CommonIndexValue, Object>> indexFieldHandlers,
      final List<NativeFieldHandler<T, Object>> nativeFieldHandlers,
      final FieldVisibilityHandler<T, Object> fieldVisiblityHandler,
      final Object defaultTypeData) {
    this.nativeFieldHandlers = nativeFieldHandlers;
    this.fieldVisiblityHandler = fieldVisiblityHandler;
    init(indexFieldHandlers, defaultTypeData);
  }

  protected void init(
      final List<? extends IndexFieldHandler<T, ? extends CommonIndexValue, Object>> indexFieldHandlers,
      final Object defaultIndexHandlerData) {
    fieldNameMatchingFieldHandlers =
        new HashMap<String, IndexFieldHandler<T, ? extends CommonIndexValue, Object>>();
    typeMatchingFieldHandlers =
        new HashMap<Class<?>, IndexFieldHandler<T, ? extends CommonIndexValue, Object>>();

    // --------------------------------------------------------------------
    // split out the dimension-matching index handlers from the
    // type-matching index handlers

    for (final IndexFieldHandler<T, ? extends CommonIndexValue, Object> indexHandler : indexFieldHandlers) {
      if (indexHandler instanceof DimensionMatchingIndexFieldHandler) {
        final String[] matchedDimensionFieldNames =
            ((DimensionMatchingIndexFieldHandler<T, ? extends CommonIndexValue, Object>) indexHandler).getSupportedIndexFieldNames();

        for (final String matchedDimensionId : matchedDimensionFieldNames) {
          fieldNameMatchingFieldHandlers.put(matchedDimensionId, indexHandler);
        }

      } else {
        // alternatively we could put make the dimension-matching field
        // handlers match types as a last resort rather than this else,
        // but they shouldn't conflict with an existing type matching
        // class

        typeMatchingFieldHandlers.put(
            GenericTypeResolver.resolveTypeArguments(
                indexHandler.getClass(),
                IndexFieldHandler.class)[1],
            indexHandler);
      }
    }

    // --------------------------------------------------------------------
    // add default handlers if the type is not already within the custom
    // handlers

    final List<IndexFieldHandler<T, ? extends CommonIndexValue, Object>> defaultTypeMatchingHandlers =
        getDefaultTypeMatchingHandlers(defaultIndexHandlerData);

    for (final IndexFieldHandler<T, ? extends CommonIndexValue, Object> defaultFieldHandler : defaultTypeMatchingHandlers) {
      final Class<?> defaultFieldHandlerClass =
          GenericTypeResolver.resolveTypeArguments(
              defaultFieldHandler.getClass(),
              IndexFieldHandler.class)[1];

      // if the type matching handlers can already handle this class,
      // don't overload it, otherwise, use this as a default handler

      final IndexFieldHandler<T, ? extends CommonIndexValue, Object> existingTypeHandler =
          FieldUtils.getAssignableValueFromClassMap(
              defaultFieldHandlerClass,
              typeMatchingFieldHandlers);
      if (existingTypeHandler == null) {
        typeMatchingFieldHandlers.put(defaultFieldHandlerClass, defaultFieldHandler);
      }
    }
  }

  /**
   * Returns an empty list of IndexFieldHandlers as default.
   *
   * @param defaultIndexHandlerData - object parameter
   * @return Empty list of IndexFieldHandlers.
   */
  protected List<IndexFieldHandler<T, ? extends CommonIndexValue, Object>> getDefaultTypeMatchingHandlers(
      final Object defaultIndexHandlerData) {
    return new ArrayList<IndexFieldHandler<T, ? extends CommonIndexValue, Object>>();
  }

  @Override
  public AdapterPersistenceEncoding encode(final T entry, final CommonIndexModel indexModel) {
    final PersistentDataset<CommonIndexValue> indexData = new PersistentDataset<CommonIndexValue>();
    final Set<String> nativeFieldsInIndex = new HashSet<String>();

    for (final NumericDimensionField<? extends CommonIndexValue> dimension : indexModel.getDimensions()) {

      final IndexFieldHandler<T, ? extends CommonIndexValue, Object> fieldHandler =
          getFieldHandler(dimension);

      if (fieldHandler == null) {
        if (LOGGER.isInfoEnabled()) {
          // Don't waste time converting IDs to String if "info" level
          // is not enabled
          LOGGER.info("Unable to find field handler for field '" + dimension.getFieldName());
        }
        continue;
      }

      final CommonIndexValue value = fieldHandler.toIndexValue(entry);
      indexData.addValue(dimension.getFieldName(), value);
      nativeFieldsInIndex.addAll(Arrays.asList(fieldHandler.getNativeFieldNames()));
    }

    final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();

    // now for the other data
    if (nativeFieldHandlers != null) {
      for (final NativeFieldHandler<T, Object> fieldHandler : nativeFieldHandlers) {
        final String fieldName = fieldHandler.getFieldName();
        if (nativeFieldsInIndex.contains(fieldName)) {
          continue;
        }
        extendedData.addValue(fieldName, fieldHandler.getFieldValue(entry));
      }
    }

    return new AdapterPersistenceEncoding(getDataId(entry), indexData, extendedData);
  }

  @SuppressWarnings("unchecked")
  @Override
  public T decode(final IndexedAdapterPersistenceEncoding data, final Index index) {
    final RowBuilder<T, Object> builder = newBuilder();
    if (index != null) {
      final CommonIndexModel indexModel = index.getIndexModel();
      for (final NumericDimensionField<? extends CommonIndexValue> dimension : indexModel.getDimensions()) {
        final IndexFieldHandler<T, CommonIndexValue, Object> fieldHandler =
            (IndexFieldHandler<T, CommonIndexValue, Object>) getFieldHandler(dimension);
        if (fieldHandler == null) {
          if (LOGGER.isInfoEnabled()) {
            // dont waste time converting IDs to String if info is
            // not
            // enabled
            LOGGER.info(
                "Unable to find field handler for data adapter '"
                    + getTypeName()
                    + "' and indexed field '"
                    + dimension.getFieldName());
          }
          continue;
        }
        String fieldName = dimension.getFieldName();
        final CommonIndexValue value = data.getCommonData().getValue(fieldName);
        if (value == null) {
          continue;
        }
        final PersistentValue<Object>[] values = fieldHandler.toNativeValues(value);
        if ((values != null) && (values.length > 0)) {
          for (final PersistentValue<Object> v : values) {
            builder.setField(v.getFieldName(), v.getValue());
          }
        }
      }
    }
    builder.setFields(data.getAdapterExtendedData().getValues());
    return builder.buildRow(data.getDataId());
  }

  protected abstract RowBuilder<T, Object> newBuilder();

  /**
   * Get index field handler for the provided dimension.
   *
   * @param dimension
   * @return field handler
   */
  public IndexFieldHandler<T, ? extends CommonIndexValue, Object> getFieldHandler(
      final NumericDimensionField<? extends CommonIndexValue> dimension) {
    // first try explicit dimension matching
    IndexFieldHandler<T, ? extends CommonIndexValue, Object> fieldHandler =
        fieldNameMatchingFieldHandlers.get(dimension.getFieldName());
    if (fieldHandler == null) {
      // if that fails, go for type matching
      fieldHandler =
          FieldUtils.getAssignableValueFromClassMap(
              GenericTypeResolver.resolveTypeArgument(
                  dimension.getClass(),
                  NumericDimensionField.class),
              typeMatchingFieldHandlers);
      fieldNameMatchingFieldHandlers.put(dimension.getFieldName(), fieldHandler);
    }
    return fieldHandler;
  }

  @Override
  public byte[] toBinary() {
    // run through the list of field handlers and persist whatever is
    // persistable, if the field handler is not persistable the assumption
    // is that the data adapter can re-create it by some other means

    // use a linked hashset to maintain order and ensure no duplication
    final Set<Persistable> persistables = new LinkedHashSet<Persistable>();
    for (final NativeFieldHandler<T, Object> nativeHandler : nativeFieldHandlers) {
      if (nativeHandler instanceof Persistable) {
        persistables.add((Persistable) nativeHandler);
      }
    }
    for (final IndexFieldHandler<T, ? extends CommonIndexValue, Object> indexHandler : typeMatchingFieldHandlers.values()) {
      if (indexHandler instanceof Persistable) {
        persistables.add((Persistable) indexHandler);
      }
    }
    for (final IndexFieldHandler<T, ? extends CommonIndexValue, Object> indexHandler : fieldNameMatchingFieldHandlers.values()) {
      if (indexHandler instanceof Persistable) {
        persistables.add((Persistable) indexHandler);
      }
    }
    if (fieldVisiblityHandler instanceof Persistable) {
      persistables.add((Persistable) fieldVisiblityHandler);
    }

    final byte[] defaultTypeDataBinary = defaultTypeDataToBinary();
    final byte[] persistablesBytes = PersistenceUtils.toBinary(persistables);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            defaultTypeDataBinary.length
                + persistablesBytes.length
                + VarintUtils.unsignedIntByteLength(defaultTypeDataBinary.length));
    VarintUtils.writeUnsignedInt(defaultTypeDataBinary.length, buf);
    buf.put(defaultTypeDataBinary);
    buf.put(persistablesBytes);
    return buf.array();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void fromBinary(final byte[] bytes) {
    if ((bytes == null) || (bytes.length == 0)) {
      LOGGER.warn("Unable to deserialize data adapter.  Binary is incomplete.");
      return;
    }
    final List<IndexFieldHandler<T, CommonIndexValue, Object>> indexFieldHandlers =
        new ArrayList<IndexFieldHandler<T, CommonIndexValue, Object>>();
    final List<NativeFieldHandler<T, Object>> nativeFieldHandlers =
        new ArrayList<NativeFieldHandler<T, Object>>();
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final byte[] defaultTypeDataBinary = new byte[VarintUtils.readUnsignedInt(buf)];
    Object defaultTypeData = null;
    if (defaultTypeDataBinary.length > 0) {
      buf.get(defaultTypeDataBinary);
      defaultTypeData = defaultTypeDataFromBinary(defaultTypeDataBinary);
    }

    final byte[] persistablesBytes = new byte[buf.remaining()];
    if (persistablesBytes.length > 0) {
      buf.get(persistablesBytes);
      final List<Persistable> persistables = PersistenceUtils.fromBinaryAsList(persistablesBytes);
      for (final Persistable persistable : persistables) {
        if (persistable instanceof IndexFieldHandler) {
          indexFieldHandlers.add((IndexFieldHandler<T, CommonIndexValue, Object>) persistable);
        }
        // in case persistable is polymorphic and multi-purpose, check
        // both
        // handler types and visibility handler
        if (persistable instanceof NativeFieldHandler) {
          nativeFieldHandlers.add((NativeFieldHandler<T, Object>) persistable);
        }
        if (persistable instanceof FieldVisibilityHandler) {
          fieldVisiblityHandler = (FieldVisibilityHandler<T, Object>) persistable;
        }
      }
    }

    this.nativeFieldHandlers = nativeFieldHandlers;
    init(indexFieldHandlers, defaultTypeData);
  }

  public FieldVisibilityHandler<T, Object> getFieldVisiblityHandler() {
    return fieldVisiblityHandler;
  }

  protected byte[] defaultTypeDataToBinary() {
    return new byte[] {};
  }

  protected Object defaultTypeDataFromBinary(final byte[] bytes) {
    return null;
  }
}
