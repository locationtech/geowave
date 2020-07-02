/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.google.common.collect.Maps;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Aggregation;

/**
 * A Meta-Aggregation, to be used internally by an aggregation query. <p> This takes an
 * aggregation-supplier and a binning strategy. When new data is aggregated, it is binned, and if
 * that bin does not exist, a new one will be made, along with a new aggregation. <p> See
 * {@link org.locationtech.geowave.core.store.api.AggregationQueryBuilder#buildWithBinningStrategy(AggregationBinningStrategy, int)}
 * AggregationQueryBuilder#bin} for usage
 *
 * @param <P> The configuration parameters of the inner aggregation.
 * @param <R> The type of the result that is returned by the inner aggregation.
 * @param <T> The type of the data given to the aggregation.
 */
public class BinningAggregation<P extends Persistable, R, T> implements
    Aggregation<BinningAggregationOptions<P, T>, Map<String, R>, T> {

  /**
   * An Aggregation that doesn't get used for aggregation, but to forward various helper tasks with,
   * such as merging and persistence.
   */
  private Aggregation<P, R, T> helperAggregation;

  /**
   * The bins and their aggregations. This is not the final result, but will be used to compute it.
   */
  private Map<String, Aggregation<P, R, T>> result;

  /**
   * The options that are needed to produce a correct aggregation.
   */
  private BinningAggregationOptions<P, T> options;

  /**
   * Create an useless BinningAggregation that must be fully realized through
   */
  public BinningAggregation() {
    this(null, null, -1);
  }

  /**
   * Creates a BinningAggregation based upon a base aggregation and a strategy for binning.
   *
   * @param baseAggregation A supplier of the inner aggregation. This decides what is done to the
   *        data inside of the bin. Make sure that the given aggregation properly implements
   *        {@link Aggregation#fromBinary(byte[]) Aggregation#fromBinary}
   *        {@link Aggregation#toBinary() Aggregation#toBinary}.
   * @param binningStrategy How to bin the given data.
   * @param maxBins The maximum amount of bins that this aggregation should support. If a bin is
   *        computed after reaching the max, it will be silently dropped.
   */
  public BinningAggregation(
      Aggregation<P, R, T> baseAggregation,
      AggregationBinningStrategy<T> binningStrategy,
      int maxBins) {
    this.options =
        new BinningAggregationOptions<>(
            PersistenceUtils.toBinary(baseAggregation),
            baseAggregation == null ? null : baseAggregation.getParameters(),
            binningStrategy,
            maxBins);
    this.result = Maps.newHashMapWithExpectedSize(maxBins == -1 ? 1024 : maxBins);
  }

  @Override
  public Map<String, R> getResult() {
    return this.result.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getResult()));
  }

  @Override
  public Map<String, R> merge(Map<String, R> result1, Map<String, R> result2) {
    final Aggregation<P, R, T> agg = this.getHelperAggregation();

    return Stream.of(result1, result2).flatMap(m -> m.entrySet().stream()).collect(
        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, agg::merge));
  }

  @Override
  public void aggregate(T entry) {
    final String[] bins = this.options.binningStrategy.binEntry(entry);
    for (String bin : bins) {
      if (this.result.containsKey(bin)) {
        this.result.get(bin).aggregate(entry);
      } else if (this.options.maxBins == -1 || this.result.size() < this.options.maxBins) {
        this.result.put(bin, this.instantiateBaseAggregation());
        this.result.get(bin).aggregate(entry);
      }
    }
  }

  /**
   * Clear all bins and all sub-aggregations. Future calls to aggregate will be unaffected by past
   * calls, after calling this.
   */
  @Override
  public void clearResult() {
    this.result.clear();
  }

  /**
   * @return A fresh instance of the base aggregation for use in this class.
   */
  private Aggregation<P, R, T> instantiateBaseAggregation() {
    Aggregation<P, R, T> agg =
        (Aggregation<P, R, T>) PersistenceUtils.fromBinary(this.options.baseBytes);
    agg.setParameters(this.options.baseParams);
    return agg;
  }

  @Override
  public BinningAggregationOptions<P, T> getParameters() {
    return this.options;
  }

  @Override
  public void setParameters(BinningAggregationOptions<P, T> parameters) {
    this.options = parameters;
  }

  @Override
  public byte[] resultToBinary(Map<String, R> result) {
    final Aggregation<P, R, T> agg = this.getHelperAggregation();
    Map<String, byte[]> mapped =
        result.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, e -> agg.resultToBinary(e.getValue())));
    int totalDataSize =
        mapped.entrySet().stream().map(e -> 8 + e.getKey().length() + e.getValue().length).reduce(
            0,
            Integer::sum);
    ByteBuffer bb = ByteBuffer.allocate(4 + totalDataSize);
    bb.putInt(mapped.size());
    mapped.forEach(
        (k, v) -> bb.putInt(k.length()).put(StringUtils.stringToBinary(k)).putInt(v.length).put(v));
    return bb.array();
  }

  @Override
  public Map<String, R> resultFromBinary(byte[] binary) {
    final Aggregation<P, R, T> agg = this.getHelperAggregation();
    ByteBuffer bb = ByteBuffer.wrap(binary);
    int mapSize = bb.getInt();
    Map<String, R> resultMap = Maps.newHashMapWithExpectedSize(mapSize);
    for (int i = 0; i < mapSize; i++) {
      int keyLen = bb.getInt();
      byte[] keyBytes = new byte[keyLen];
      bb.get(keyBytes);
      String key = StringUtils.stringFromBinary(keyBytes);

      int valLen = bb.getInt();
      byte[] valBytes = new byte[valLen];
      bb.get(valBytes);
      R val = agg.resultFromBinary(valBytes);

      resultMap.put(key, val);
    }
    return resultMap;
  }

  // TODO after rebasing with Rich's PR, these can be removed.
  @Override
  public byte[] toBinary() {
    // the parameters and results are all that are needed to persist this object,
    // and they are serialized in their own methods.
    // we dont need to serialize anything else.
    return new byte[0];
  }

  @Override
  public void fromBinary(byte[] bytes) {}

  private Aggregation<P, R, T> getHelperAggregation() {
    if (this.helperAggregation == null) {
      this.helperAggregation = this.instantiateBaseAggregation();
    }
    return this.helperAggregation;
  }
}
