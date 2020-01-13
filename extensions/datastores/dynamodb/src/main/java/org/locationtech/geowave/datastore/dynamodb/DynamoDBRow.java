/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.bouncycastle.util.Arrays;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.entities.MergeableGeoWaveRow;
import org.locationtech.geowave.datastore.dynamodb.util.DynamoDBUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.beust.jcommander.internal.Lists;

public class DynamoDBRow extends MergeableGeoWaveRow implements GeoWaveRow {
  public static final String GW_PARTITION_ID_KEY = "P";
  public static final String GW_RANGE_KEY = "R";
  public static final String GW_FIELD_MASK_KEY = "F";
  public static final String GW_VISIBILITY_KEY = "X";
  public static final String GW_VALUE_KEY = "V";

  private final GeoWaveKey key;

  private final List<Map<String, AttributeValue>> objMaps = Lists.newArrayList();

  public DynamoDBRow(final Map<String, AttributeValue> objMap) {
    super(getFieldValues(objMap));

    objMaps.add(objMap);
    key = getGeoWaveKey(objMap);
  }

  private static GeoWaveValue[] getFieldValues(final Map<String, AttributeValue> objMap) {
    final GeoWaveValue[] fieldValues = new GeoWaveValueImpl[1];
    final AttributeValue fieldMaskAttr = objMap.get(GW_FIELD_MASK_KEY);
    final byte[] fieldMask = fieldMaskAttr == null ? null : fieldMaskAttr.getB().array();

    final AttributeValue visibilityAttr = objMap.get(GW_VISIBILITY_KEY);
    final byte[] visibility = visibilityAttr == null ? null : visibilityAttr.getB().array();

    final AttributeValue valueAttr = objMap.get(GW_VALUE_KEY);
    final byte[] value = valueAttr == null ? null : valueAttr.getB().array();

    fieldValues[0] = new GeoWaveValueImpl(fieldMask, visibility, value);
    return fieldValues;
  }

  private static GeoWaveKey getGeoWaveKey(final Map<String, AttributeValue> objMap) {
    final byte[] partitionKey = objMap.get(GW_PARTITION_ID_KEY).getB().array();

    final byte[] rangeKey = objMap.get(GW_RANGE_KEY).getB().array();
    final int length = rangeKey.length;

    final ByteBuffer metadataBuf = ByteBuffer.wrap(rangeKey, length - 8, 8);
    final int dataIdLength = metadataBuf.getInt();
    final int numberOfDuplicates = metadataBuf.getInt();

    final ByteBuffer buf = ByteBuffer.wrap(rangeKey, 0, length - 16);
    final byte[] sortKey = new byte[length - 16 - 2 - dataIdLength];
    final byte[] dataId = new byte[dataIdLength];

    // Range key (row ID) = adapterId + sortKey + dataId
    final byte[] internalAdapterIdBytes = new byte[2];
    buf.get(internalAdapterIdBytes);
    final short internalAdapterId = ByteArrayUtils.byteArrayToShort(internalAdapterIdBytes);
    buf.get(sortKey);
    buf.get(dataId);

    return new GeoWaveKeyImpl(
        dataId,
        internalAdapterId,
        Arrays.areEqual(DynamoDBUtils.EMPTY_PARTITION_KEY, partitionKey) ? new byte[0]
            : partitionKey,
        DynamoDBUtils.decodeSortableBase64(sortKey),
        numberOfDuplicates);
  }

  public List<Map<String, AttributeValue>> getAttributeMapping() {
    return objMaps;
  }

  public static class GuavaRowTranslationHelper implements
      Function<Map<String, AttributeValue>, DynamoDBRow> {
    @Override
    public DynamoDBRow apply(final Map<String, AttributeValue> input) {
      return new DynamoDBRow(input);
    }
  }

  @Override
  public byte[] getDataId() {
    return key.getDataId();
  }

  @Override
  public short getAdapterId() {
    return key.getAdapterId();
  }

  @Override
  public byte[] getSortKey() {
    return key.getSortKey();
  }

  @Override
  public byte[] getPartitionKey() {
    return key.getPartitionKey();
  }

  @Override
  public int getNumberOfDuplicates() {
    return key.getNumberOfDuplicates();
  }

  @Override
  public void mergeRowInternal(final MergeableGeoWaveRow row) {
    if (row instanceof DynamoDBRow) {
      objMaps.addAll(((DynamoDBRow) row).getAttributeMapping());
    }
  }

  public static byte[] getRangeKey(final GeoWaveKey key) {
    final byte[] sortKey = DynamoDBUtils.encodeSortableBase64(key.getSortKey());
    final ByteBuffer buffer = ByteBuffer.allocate(sortKey.length + key.getDataId().length + 18);
    buffer.put(ByteArrayUtils.shortToByteArray(key.getAdapterId()));
    buffer.put(sortKey);
    buffer.put(key.getDataId());
    buffer.putLong(Long.MAX_VALUE - System.nanoTime());
    buffer.putInt(key.getDataId().length);
    buffer.putInt(key.getNumberOfDuplicates());
    buffer.rewind();

    return buffer.array();
  }
}
