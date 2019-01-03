/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.iterators;

import com.google.common.io.BaseEncoding;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleEntryFilterIterator extends Filter {
  private static final Logger LOGGER = LoggerFactory.getLogger(SingleEntryFilterIterator.class);
  public static final String ENTRY_FILTER_ITERATOR_NAME = "GEOWAVE_ENTRY_FILTER_ITERATOR";
  public static final int ENTRY_FILTER_ITERATOR_PRIORITY = 25;
  public static final String WHOLE_ROW_ITERATOR_NAME = "GEOWAVE_WHOLE_ROW_ITERATOR";
  public static final int WHOLE_ROW_ITERATOR_PRIORITY = ENTRY_FILTER_ITERATOR_PRIORITY - 1;
  public static final String ADAPTER_ID = "adapterid";
  public static final String DATA_IDS = "dataids";
  public static final String WHOLE_ROW_ENCODED_KEY = "wholerow";
  private boolean wholeRowEncoded;
  private byte[] adapterId;
  private List<byte[]> dataIds;

  @Override
  public boolean accept(final Key k, final Value v) {

    boolean accept = true;

    Map<Key, Value> entries = null;
    if (wholeRowEncoded) {
      try {
        entries = WholeRowIterator.decodeRow(k, v);
      } catch (final IOException e) {
        LOGGER.error("Unable to decode row.", e);
        return false;
      }
    } else {
      entries = new HashMap<Key, Value>();
      entries.put(k, v);
    }
    if ((entries != null) && entries.isEmpty()) {
      accept = false;
    } else {
      if (entries == null) {
        LOGGER.error("Internal error in iterator - entries map null when it shouldn't be");
        return false;
      }
      for (final Key key : entries.keySet()) {
        final byte[] localAdapterId = key.getColumnFamilyData().getBackingArray();

        if (Arrays.equals(localAdapterId, adapterId)) {
          final byte[] accumRowId = key.getRowData().getBackingArray();

          final byte[] metadata =
              Arrays.copyOfRange(accumRowId, accumRowId.length - 12, accumRowId.length);

          final ByteBuffer metadataBuf = ByteBuffer.wrap(metadata);
          final int adapterIdLength = metadataBuf.getInt();
          final int dataIdLength = metadataBuf.getInt();

          final ByteBuffer buf = ByteBuffer.wrap(accumRowId, 0, accumRowId.length - 12);
          final byte[] indexId = new byte[accumRowId.length - 12 - adapterIdLength - dataIdLength];
          final byte[] rawAdapterId = new byte[adapterIdLength];
          final byte[] rawDataId = new byte[dataIdLength];
          buf.get(indexId);
          buf.get(rawAdapterId);
          buf.get(rawDataId);

          accept = false;
          for (final byte[] dataId : dataIds) {
            if (Arrays.equals(rawDataId, dataId) && Arrays.equals(rawAdapterId, adapterId)) {
              accept |= true;
            }
          }
        } else {
          accept = false;
        }
      }
    }

    return accept;
  }

  public static final String encodeIDs(final List<ByteArray> dataIds) {
    int size = VarintUtils.unsignedIntByteLength(dataIds.size());
    for (final ByteArray id : dataIds) {
      size += id.getBytes().length + VarintUtils.unsignedIntByteLength(id.getBytes().length);
    }
    final ByteBuffer buffer = ByteBuffer.allocate(size);
    VarintUtils.writeUnsignedInt(dataIds.size(), buffer);
    for (final ByteArray id : dataIds) {
      final byte[] sId = id.getBytes();
      VarintUtils.writeUnsignedInt(sId.length, buffer);
      buffer.put(sId);
    }

    return ByteArrayUtils.byteArrayToString(buffer.array());
  }

  private static final List<byte[]> decodeIDs(final String dataIdsString) {
    final ByteBuffer buf = ByteBuffer.wrap(ByteArrayUtils.byteArrayFromString(dataIdsString));
    final List<byte[]> list = new ArrayList<byte[]>();
    int count = VarintUtils.readUnsignedInt(buf);
    while (count > 0) {
      final byte[] tempByte = new byte[VarintUtils.readUnsignedInt(buf)];
      buf.get(tempByte);
      list.add(tempByte);
      count--;
    }
    return list;
  }

  @Override
  public void init(
      final SortedKeyValueIterator<Key, Value> source,
      final Map<String, String> options,
      final IteratorEnvironment env) throws IOException {

    final String adapterIdStr = options.get(ADAPTER_ID);
    final String dataIdsStr = options.get(DATA_IDS);
    if (adapterIdStr == null) {
      throw new IllegalArgumentException(
          "'adapterid' must be set for " + SingleEntryFilterIterator.class.getName());
    }
    if (dataIdsStr == null) {
      throw new IllegalArgumentException(
          "'dataid' must be set for " + SingleEntryFilterIterator.class.getName());
    }

    adapterId = BaseEncoding.base64Url().decode(adapterIdStr);
    dataIds = decodeIDs(dataIdsStr);
    final String wholeRowEncodedStr = options.get(WHOLE_ROW_ENCODED_KEY);
    // default to whole row encoded if not specified
    wholeRowEncoded =
        (wholeRowEncodedStr == null || !wholeRowEncodedStr.equals(Boolean.toString(false)));
    super.init(source, options, env);
  }
}
