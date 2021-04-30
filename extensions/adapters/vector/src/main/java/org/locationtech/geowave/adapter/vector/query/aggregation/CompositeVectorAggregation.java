/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.aggregation;

import java.nio.ByteBuffer;
import java.util.List;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistableList;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;


/**
 * Aggregation class that allows multiple aggregations to be performed in a single aggregation
 * query. The initial implementation does not take advantage of common index aggregations.
 *
 * TODO: Update this class to derive from BaseOptimalVectorAggregation and if all sub aggregations
 * are common index aggregations, then the composite aggregation can run with only common index
 * data. Otherwise the feature needs to be decoded anyways, so all of the sub aggregations should be
 * run on the decoded data.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class CompositeVectorAggregation implements
    Aggregation<PersistableList, List<Object>, SimpleFeature> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompositeVectorAggregation.class);

  List<Aggregation> aggregations = Lists.newArrayList();

  /**
   * Add an aggregation to this composite aggregation.
   *
   * @param aggregation the aggregation to add
   */
  public void add(final Aggregation<?, ?, SimpleFeature> aggregation) {
    aggregations.add(aggregation);
  }

  @Override
  public PersistableList getParameters() {
    final List<Persistable> persistables = Lists.newArrayListWithCapacity(aggregations.size());
    for (final Aggregation agg : aggregations) {
      persistables.add(agg);
      persistables.add(agg.getParameters());
    }
    return new PersistableList(persistables);
  }

  @Override
  public void setParameters(final PersistableList parameters) {
    final List<Persistable> persistables = parameters.getPersistables();
    final int numAggregations = persistables.size() / 2;
    aggregations = Lists.newArrayListWithCapacity(numAggregations);
    for (int i = 0; i < numAggregations; i++) {
      final Aggregation aggregation = (Aggregation) persistables.get(i * 2);
      aggregation.setParameters(persistables.get((i * 2) + 1));
      aggregations.add(aggregation);
    }
  }

  @Override
  public List<Object> merge(final List<Object> result1, final List<Object> result2) {
    final List<Object> merged = Lists.newArrayListWithCapacity(aggregations.size());
    for (int i = 0; i < aggregations.size(); i++) {
      merged.add(aggregations.get(i).merge(result1.get(i), result2.get(i)));
    }
    return merged;
  }

  @Override
  public List<Object> getResult() {
    return Lists.transform(aggregations, a -> a.getResult());
  }

  @Override
  public byte[] resultToBinary(final List<Object> result) {
    final List<byte[]> parts = Lists.newArrayListWithCapacity(aggregations.size());
    int length = 0;
    for (int i = 0; i < aggregations.size(); i++) {
      final byte[] binary = aggregations.get(i).resultToBinary(result.get(i));
      length += binary.length + 4;
      parts.add(binary);
    }
    final ByteBuffer buffer = ByteBuffer.allocate(length);
    for (final byte[] part : parts) {
      buffer.putInt(part.length);
      buffer.put(part);
    }
    return buffer.array();
  }

  @Override
  public List<Object> resultFromBinary(final byte[] binary) {
    final ByteBuffer buffer = ByteBuffer.wrap(binary);
    final List<Object> result = Lists.newArrayListWithCapacity(aggregations.size());
    final int length = aggregations.size();
    for (int i = 0; i < length; i++) {
      final int partLength = buffer.getInt();
      final byte[] part = new byte[partLength];
      buffer.get(part);
      result.add(aggregations.get(i).resultFromBinary(part));
    }
    return result;
  }

  @Override
  public void clearResult() {
    aggregations.forEach(a -> a.clearResult());
  }

  @Override
  public void aggregate(final DataTypeAdapter<SimpleFeature> adapter, final SimpleFeature entry) {
    aggregations.forEach(a -> a.aggregate(adapter, entry));
  }


}
