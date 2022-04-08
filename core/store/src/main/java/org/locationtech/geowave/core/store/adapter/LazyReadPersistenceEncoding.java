/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.util.List;
import java.util.function.Supplier;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.data.MultiFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.flatten.BitmaskUtils;
import org.locationtech.geowave.core.store.flatten.FlattenedFieldInfo;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This is an implements of persistence encoding that also contains all of the extended data values
 * used to form the native type supported by this adapter. It also contains information about the
 * persisted object within a particular index such as the insertion ID in the index and the number
 * of duplicates for this entry in the index, and is used when reading data from the index.
 */
public class LazyReadPersistenceEncoding extends IndexedAdapterPersistenceEncoding {
  private FieldValueReader deferredFieldReader;

  public LazyReadPersistenceEncoding(
      final short adapterId,
      final byte[] dataId,
      final byte[] partitionKey,
      final byte[] sortKey,
      final int duplicateCount,
      final InternalDataAdapter<?> dataAdapter,
      final CommonIndexModel indexModel,
      final AdapterToIndexMapping indexMapping,
      final byte[] fieldSubsetBitmask,
      final GeoWaveValue[] fieldValues,
      final boolean isSecondaryIndex) {
    super(
        adapterId,
        dataId,
        partitionKey,
        sortKey,
        duplicateCount,
        new MultiFieldPersistentDataset<>(),
        new MultiFieldPersistentDataset<byte[]>(),
        new MultiFieldPersistentDataset<>());
    deferredFieldReader =
        new InstanceFieldValueReader(
            fieldSubsetBitmask,
            dataAdapter,
            indexModel,
            indexMapping,
            fieldValues,
            isSecondaryIndex);
  }

  public LazyReadPersistenceEncoding(
      final short adapterId,
      final byte[] dataId,
      final byte[] partitionKey,
      final byte[] sortKey,
      final int duplicateCount,
      final InternalDataAdapter<?> dataAdapter,
      final CommonIndexModel indexModel,
      final AdapterToIndexMapping indexMapping,
      final byte[] fieldSubsetBitmask,
      final Supplier<GeoWaveValue[]> fieldValues) {
    super(
        adapterId,
        dataId,
        partitionKey,
        sortKey,
        duplicateCount,
        new MultiFieldPersistentDataset<>(),
        new MultiFieldPersistentDataset<byte[]>(),
        new MultiFieldPersistentDataset<>());
    deferredFieldReader =
        new SupplierFieldValueReader(
            fieldSubsetBitmask,
            dataAdapter,
            indexModel,
            indexMapping,
            fieldValues,
            true);
  }

  @Override
  public PersistentDataset<Object> getAdapterExtendedData() {
    // defer any reading of fieldValues until necessary
    deferredReadFields();
    return super.getAdapterExtendedData();
  }

  @Override
  public PersistentDataset<byte[]> getUnknownData() {
    // defer any reading of fieldValues until necessary
    deferredReadFields();
    return super.getUnknownData();
  }

  @Override
  public PersistentDataset<Object> getCommonData() {
    // defer any reading of fieldValues until necessary
    deferredReadFields();
    return super.getCommonData();
  }

  @SuppressFBWarnings(justification = "This is intentional to avoid unnecessary sync")
  private void deferredReadFields() {
    if (deferredFieldReader != null) {
      // this is intentional to check for null twice to avoid extra unnecessary synchronization
      synchronized (this) {
        if (deferredFieldReader != null) {
          deferredFieldReader.readValues();
          deferredFieldReader = null;
        }
      }
    }
  }

  private abstract class FieldValueReader {
    private final byte[] fieldSubsetBitmask;
    private final InternalDataAdapter<?> dataAdapter;
    private final CommonIndexModel indexModel;
    private final AdapterToIndexMapping indexMapping;
    private final boolean isSecondaryIndex;

    public FieldValueReader(
        final byte[] fieldSubsetBitmask,
        final InternalDataAdapter<?> dataAdapter,
        final CommonIndexModel indexModel,
        final AdapterToIndexMapping indexMapping,
        final boolean isSecondaryIndex) {
      super();
      this.fieldSubsetBitmask = fieldSubsetBitmask;
      this.dataAdapter = dataAdapter;
      this.indexModel = indexModel;
      this.indexMapping = indexMapping;
      this.isSecondaryIndex = isSecondaryIndex;
    }

    protected void readValues() {
      for (final GeoWaveValue value : getFieldValues()) {
        byte[] byteValue = value.getValue();
        byte[] fieldMask = value.getFieldMask();
        if (fieldSubsetBitmask != null) {
          final byte[] newBitmask = BitmaskUtils.generateANDBitmask(fieldMask, fieldSubsetBitmask);
          byteValue = BitmaskUtils.constructNewValue(byteValue, fieldMask, newBitmask);
          if ((byteValue == null) || (byteValue.length == 0)) {
            continue;
          }
          fieldMask = newBitmask;
        }
        readValue(new GeoWaveValueImpl(fieldMask, value.getVisibility(), byteValue));
      }
    }

    abstract protected GeoWaveValue[] getFieldValues();

    private void readValue(final GeoWaveValue value) {
      final List<FlattenedFieldInfo> fieldInfos =
          DataStoreUtils.decomposeFlattenedFields(
              value.getFieldMask(),
              value.getValue(),
              value.getVisibility(),
              -2).getFieldsRead();
      for (final FlattenedFieldInfo fieldInfo : fieldInfos) {
        final String fieldName =
            dataAdapter.getFieldNameForPosition(
                isSecondaryIndex ? DataIndexUtils.DATA_ID_INDEX.getIndexModel() : indexModel,
                fieldInfo.getFieldPosition());
        FieldReader<Object> indexFieldReader = null;
        if (!isSecondaryIndex) {
          indexFieldReader = indexModel.getReader(fieldName);
        }
        if (indexFieldReader != null) {
          final Object indexValue = indexFieldReader.readField(fieldInfo.getValue());
          commonData.addValue(fieldName, indexValue);
        } else {
          final FieldReader<?> extFieldReader = dataAdapter.getReader(fieldName);
          if (extFieldReader != null) {
            final Object objValue = extFieldReader.readField(fieldInfo.getValue());
            // TODO GEOWAVE-1018, do we care about visibility
            adapterExtendedData.addValue(fieldName, objValue);
          } else {
            LOGGER.error("field reader not found for data entry, the value may be ignored");
            unknownData.addValue(fieldName, fieldInfo.getValue());
          }
        }
      }
      if (isSecondaryIndex) {
        for (IndexFieldMapper<?, ?> mapper : indexMapping.getIndexFieldMappers()) {
          final Object commonIndexValue =
              InternalAdapterUtils.entryToIndexValue(
                  mapper,
                  dataAdapter.getAdapter(),
                  adapterExtendedData);
          commonData.addValue(mapper.indexFieldName(), commonIndexValue);
        }
      }
    }
  }
  private class InstanceFieldValueReader extends FieldValueReader {
    private final GeoWaveValue[] fieldValues;

    public InstanceFieldValueReader(
        final byte[] fieldSubsetBitmask,
        final InternalDataAdapter<?> dataAdapter,
        final CommonIndexModel indexModel,
        final AdapterToIndexMapping indexMapping,
        final GeoWaveValue[] fieldValues,
        final boolean isSecondaryIndex) {
      super(fieldSubsetBitmask, dataAdapter, indexModel, indexMapping, isSecondaryIndex);
      this.fieldValues = fieldValues;
    }

    @Override
    protected GeoWaveValue[] getFieldValues() {
      return fieldValues;
    }
  }
  private class SupplierFieldValueReader extends FieldValueReader {
    private final Supplier<GeoWaveValue[]> fieldValues;

    public SupplierFieldValueReader(
        final byte[] fieldSubsetBitmask,
        final InternalDataAdapter<?> dataAdapter,
        final CommonIndexModel indexModel,
        final AdapterToIndexMapping indexMapping,
        final Supplier<GeoWaveValue[]> fieldValues,
        final boolean isSecondaryIndex) {
      super(fieldSubsetBitmask, dataAdapter, indexModel, indexMapping, isSecondaryIndex);
      this.fieldValues = fieldValues;
    }

    @Override
    protected GeoWaveValue[] getFieldValues() {
      return fieldValues.get();
    }
  }
}
