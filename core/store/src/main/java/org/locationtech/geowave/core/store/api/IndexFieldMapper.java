/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.index.IndexFieldMapperRegistry;
import com.google.common.collect.Sets;

/**
 * Abstract base class for mapping one or more adapter fields to a single index field. These field
 * mappers are registered and discovered via SPI through the {@link IndexFieldMapperRegistry}.
 *
 * @param <N> the adapter field type
 * @param <I> the index field type
 */
public abstract class IndexFieldMapper<N, I> implements Persistable {
  protected String indexFieldName = null;
  protected String[] adapterFields = null;

  public final void init(
      final String indexFieldName,
      final List<FieldDescriptor<N>> inputFieldDescriptors,
      final IndexFieldOptions options) {
    this.indexFieldName = indexFieldName;
    this.adapterFields =
        inputFieldDescriptors.stream().map(FieldDescriptor::fieldName).toArray(String[]::new);
    initFromOptions(inputFieldDescriptors, options);
  }

  /**
   * Initialize the field mapper with the given field descriptors and index field options.
   * 
   * @param inputFieldDescriptors the adapter field descriptors to use in the mapping
   * @param options the index field options provided by the index
   */
  protected void initFromOptions(
      final List<FieldDescriptor<N>> inputFieldDescriptors,
      final IndexFieldOptions options) {};

  /**
   * As a performance measure, sometimes the queried data will vary from the data that was ingested.
   * For example querying a spatial index with a custom CRS will return data in that CRS, even if
   * the data was originally in a different CRS. This method transforms the adapter field
   * descriptors to appropriately represent the queried data.
   * 
   * @param fieldDescriptors the output field descriptors
   */
  public void transformFieldDescriptors(final FieldDescriptor<?>[] fieldDescriptors) {}

  /**
   * @return the adapter field names used in the mapping
   */
  public String[] getAdapterFields() {
    return adapterFields;
  }

  /**
   * @return the adapter field names used in the mapping, ordered by the index dimensions they are
   *         associated with
   */
  public String[] getIndexOrderedAdapterFields() {
    return adapterFields;
  }

  /**
   * @return the index field used in the mapping
   */
  public String indexFieldName() {
    return indexFieldName;
  }

  /**
   * Converts native field values to the value expected by the index.
   * 
   * @param nativeFieldValues the native field values
   * @return the value to use in the index
   */
  public abstract I toIndex(final List<N> nativeFieldValues);

  /**
   * Converts an index value back to the fields used by the adapter.
   * 
   * @param indexFieldValue the index value
   * @return the adapter values
   */
  public abstract void toAdapter(I indexFieldValue, RowBuilder<?> rowBuilder);

  /**
   * @return the index field type
   */
  public abstract Class<I> indexFieldType();

  /**
   * @return the adapter field type
   */
  public abstract Class<N> adapterFieldType();

  /**
   * @return a set of suggested adapter field names that might be associated with this field mapper
   */
  public Set<String> getLowerCaseSuggestedFieldNames() {
    return Sets.newHashSet();
  }

  public boolean isCompatibleWith(final Class<?> fieldClass) {
    // The logic here is that if the index field type is the same as the adapter field type, most
    // likely the field value will be directly used by the index, so the child class would be
    // preserved. If they don't match, a transformation will occur, in which case an exact match
    // would be needed to be able to transform the index value back to the appropriate adapter field
    // type.
    if (indexFieldType().equals(adapterFieldType())) {
      return adapterFieldType().isAssignableFrom(fieldClass);
    }
    return adapterFieldType().equals(fieldClass);
  }

  /**
   * @return the number of adapter fields used in the index field mapping
   */
  public abstract short adapterFieldCount();

  private byte[] indexFieldBytes = null;
  private byte[] adapterFieldsBytes = null;

  protected int byteLength() {
    indexFieldBytes = StringUtils.stringToBinary(indexFieldName);
    adapterFieldsBytes = StringUtils.stringsToBinary(adapterFields);
    return VarintUtils.unsignedShortByteLength((short) indexFieldBytes.length)
        + indexFieldBytes.length
        + VarintUtils.unsignedShortByteLength((short) adapterFieldsBytes.length)
        + adapterFieldsBytes.length;
  }

  protected void writeBytes(final ByteBuffer buffer) {
    VarintUtils.writeUnsignedShort((short) indexFieldBytes.length, buffer);
    buffer.put(indexFieldBytes);
    VarintUtils.writeUnsignedShort((short) adapterFieldsBytes.length, buffer);
    buffer.put(adapterFieldsBytes);
  }

  protected void readBytes(final ByteBuffer buffer) {
    indexFieldBytes = new byte[VarintUtils.readUnsignedShort(buffer)];
    buffer.get(indexFieldBytes);
    this.indexFieldName = StringUtils.stringFromBinary(indexFieldBytes);
    adapterFieldsBytes = new byte[VarintUtils.readUnsignedShort(buffer)];
    buffer.get(adapterFieldsBytes);
    this.adapterFields = StringUtils.stringsFromBinary(adapterFieldsBytes);
    indexFieldBytes = null;
    adapterFieldsBytes = null;
  }

  @Override
  public final byte[] toBinary() {
    final ByteBuffer buffer = ByteBuffer.allocate(byteLength());
    writeBytes(buffer);
    return buffer.array();
  }

  @Override
  public final void fromBinary(final byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    readBytes(buffer);
  }

  /**
   * Provides an open-ended interface so that custom index fields can provide any information to the
   * mapper that may be needed. One example is that spatial index fields provide CRS information to
   * spatial field mappers.
   */
  public static interface IndexFieldOptions {

  }
}
