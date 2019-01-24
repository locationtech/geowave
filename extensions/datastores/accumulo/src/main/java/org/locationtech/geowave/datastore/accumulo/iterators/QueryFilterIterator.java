/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.iterators;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.Text;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.DeferredReadCommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.flatten.FlattenedUnreadData;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.mapreduce.URLClassloaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryFilterIterator extends Filter {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryFilterIterator.class);
  public static final String QUERY_ITERATOR_NAME = "GEOWAVE_QUERY_FILTER";
  public static final int QUERY_ITERATOR_PRIORITY = 25;
  public static final String FILTER = "filter";
  public static final String MODEL = "model";
  public static final String PARTITION_KEY_LENGTH = "partitionLength";
  private QueryFilter filter;
  protected CommonIndexModel model;
  protected int partitionKeyLength = 0;
  protected Text currentRow = new Text();
  private List<String> commonIndexFieldNames = new ArrayList<>();

  static {
    initialize();
  }

  private static void initialize() {
    try {
      URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    } catch (final Error factoryError) {
      String type = "";
      Field f = null;
      try {
        f = URL.class.getDeclaredField("factory");
      } catch (final NoSuchFieldException e) {
        LOGGER.error(
            "URL.setURLStreamHandlerFactory() can only be called once per JVM instance, and currently something has set it to;  additionally unable to discover type of Factory",
            e);
        throw (factoryError);
      }

      // HP Fortify "Access Specifier Manipulation"
      // This object is being modified by trusted code,
      // in a way that is not influenced by user input
      f.setAccessible(true);
      Object o;
      try {
        o = f.get(null);
      } catch (final IllegalAccessException e) {
        LOGGER.error(
            "URL.setURLStreamHandlerFactory() can only be called once per JVM instance, and currently something has set it to;  additionally unable to discover type of Factory",
            e);
        throw (factoryError);
      }
      if (o instanceof FsUrlStreamHandlerFactory) {
        LOGGER.info(
            "setURLStreamHandlerFactory already set on this JVM to FsUrlStreamHandlerFactory.  Nothing to do");
        return;
      } else {
        type = o.getClass().getCanonicalName();
      }
      LOGGER.error(
          "URL.setURLStreamHandlerFactory() can only be called once per JVM instance, and currently something has set it to: "
              + type);
      throw (factoryError);
    }
  }

  @Override
  protected void findTop() {
    // it seems like the key can be cached and turns out to improve
    // performance a bit
    findTopEnhanced(getSource(), this);
  }

  protected static void findTopEnhanced(
      final SortedKeyValueIterator<Key, Value> source,
      final Filter filter) {
    Key key;
    if (source.hasTop()) {
      key = source.getTopKey();
    } else {
      return;
    }
    while (!key.isDeleted() && !filter.accept(key, source.getTopValue())) {
      try {
        source.next();
        if (source.hasTop()) {
          key = source.getTopKey();
        } else {
          return;
        }
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean accept(final Key key, final Value value) {
    if (isSet()) {
      final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<>();

      final FlattenedUnreadData unreadData = aggregateFieldData(key, value, commonData);
      return applyRowFilter(key.getRow(currentRow), commonData, unreadData);
    }
    // if the query filter or index model did not get sent to this iterator,
    // it'll just have to accept everything
    return true;
  }

  protected FlattenedUnreadData aggregateFieldData(
      final Key key,
      final Value value,
      final PersistentDataset<CommonIndexValue> commonData) {
    final GeoWaveKey gwKey = new GeoWaveKeyImpl(key.getRow().copyBytes(), partitionKeyLength);
    final GeoWaveValue gwValue =
        new GeoWaveValueImpl(
            key.getColumnQualifier().getBytes(),
            key.getColumnVisibilityData().getBackingArray(),
            value.get());
    return DataStoreUtils.aggregateFieldData(
        gwKey,
        gwValue,
        commonData,
        model,
        commonIndexFieldNames);
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(final IteratorEnvironment env) {
    final QueryFilterIterator iterator = new QueryFilterIterator();
    iterator.setSource(getSource().deepCopy(env));
    iterator.filter = filter;
    iterator.commonIndexFieldNames.addAll(commonIndexFieldNames);
    iterator.model = model;
    return iterator;
  }

  protected boolean applyRowFilter(
      final Text currentRow,
      final PersistentDataset<CommonIndexValue> commonData,
      final FlattenedUnreadData unreadData) {
    return applyRowFilter(getEncoding(currentRow, partitionKeyLength, commonData, unreadData));
  }

  protected static CommonIndexedPersistenceEncoding getEncoding(
      final Text currentRow,
      final int partitionKeyLength,
      final PersistentDataset<CommonIndexValue> commonData,
      final FlattenedUnreadData unreadData) {
    final GeoWaveKeyImpl rowId = new GeoWaveKeyImpl(currentRow.copyBytes(), partitionKeyLength);
    return new DeferredReadCommonIndexedPersistenceEncoding(
        rowId.getAdapterId(),
        rowId.getDataId(),
        rowId.getPartitionKey(),
        rowId.getSortKey(),
        rowId.getNumberOfDuplicates(),
        commonData,
        unreadData);
  }

  protected boolean applyRowFilter(final CommonIndexedPersistenceEncoding encoding) {
    return filter.accept(model, encoding);
  }

  public boolean isSet() {
    return (filter != null) && (model != null);
  }

  @Override
  public void init(
      final SortedKeyValueIterator<Key, Value> source,
      final Map<String, String> options,
      final IteratorEnvironment env) throws IOException {
    setOptions(options);
    super.init(source, options, env);
  }

  public void setOptions(final Map<String, String> options) {
    if (options == null) {
      throw new IllegalArgumentException(
          "Arguments must be set for " + QueryFilterIterator.class.getName());
    }
    try {
      if (options.containsKey(FILTER)) {
        final String filterStr = options.get(FILTER);
        final byte[] filterBytes = ByteArrayUtils.byteArrayFromString(filterStr);
        filter = (QueryFilter) URLClassloaderUtils.fromBinary(filterBytes);
      }
      if (options.containsKey(MODEL)) {
        final String modelStr = options.get(MODEL);
        final byte[] modelBytes = ByteArrayUtils.byteArrayFromString(modelStr);
        model = (CommonIndexModel) URLClassloaderUtils.fromBinary(modelBytes);
        commonIndexFieldNames = DataStoreUtils.getUniqueDimensionFields(model);
      }
      if (options.containsKey(PARTITION_KEY_LENGTH)) {
        final String partitionKeyLengthStr = options.get(PARTITION_KEY_LENGTH);
        partitionKeyLength = Integer.parseInt(partitionKeyLengthStr);
      }
    } catch (final Exception e) {
      throw new IllegalArgumentException(e);
    }
  }
}
