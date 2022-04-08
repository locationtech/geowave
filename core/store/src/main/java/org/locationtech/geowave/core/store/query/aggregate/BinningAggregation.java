/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.BinningStrategy;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import com.google.common.collect.Maps;

/**
 * A Meta-Aggregation, to be used internally by an aggregation query. <p> This takes an
 * aggregation-supplier and a binning strategy. When new data is aggregated, it is binned, and if
 * that bin does not exist, a new one will be made, along with a new aggregation. <p> See
 * {@link org.locationtech.geowave.core.store.api.AggregationQueryBuilder#buildWithBinningStrategy(BinningStrategy, int)}
 * AggregationQueryBuilder#bin} for usage
 *
 * @param <P> The configuration parameters of the inner aggregation.
 * @param <R> The type of the result that is returned by the inner aggregation.
 * @param <T> The type of the data given to the aggregation.
 */
public class BinningAggregation<P extends Persistable, R, T> implements
    Aggregation<BinningAggregationOptions<P, T>, Map<ByteArray, R>, T> {

  /**
   * An Aggregation that doesn't get used for aggregation, but to forward various helper tasks with,
   * such as merging and persistence.
   */
  private Aggregation<P, R, T> helperAggregation;

  /**
   * The bins and their aggregations. This is not the final result, but will be used to compute it.
   */
  private Map<ByteArray, Aggregation<P, R, T>> result;

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
      final Aggregation<P, R, T> baseAggregation,
      final BinningStrategy binningStrategy,
      final int maxBins) {
    this.options =
        new BinningAggregationOptions<>(
            PersistenceUtils.toBinary(baseAggregation),
            baseAggregation == null ? null
                : PersistenceUtils.toBinary(baseAggregation.getParameters()),
            binningStrategy,
            maxBins);
    this.result = Maps.newHashMapWithExpectedSize(maxBins == -1 ? 1024 : maxBins);
  }

  @Override
  public Map<ByteArray, R> getResult() {
    return this.result.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getResult()));
  }

  @Override
  public Map<ByteArray, R> merge(final Map<ByteArray, R> result1, final Map<ByteArray, R> result2) {
    final Aggregation<P, R, T> agg = this.getHelperAggregation();

    return Stream.of(result1, result2).flatMap(m -> m.entrySet().stream()).collect(
        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, agg::merge));
  }

  @Override
  public void aggregate(final DataTypeAdapter<T> adapter, final T entry) {
    final ByteArray[] bins = this.options.binningStrategy.getBins(adapter, entry);
    for (final ByteArray bin : bins) {
      if (this.result.containsKey(bin)) {
        this.result.get(bin).aggregate(adapter, entry);
      } else if ((this.options.maxBins == -1) || (this.result.size() < this.options.maxBins)) {
        this.result.put(bin, this.instantiateBaseAggregation());
        this.result.get(bin).aggregate(adapter, entry);
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
    final Aggregation<P, R, T> agg =
        (Aggregation<P, R, T>) PersistenceUtils.fromBinary(this.options.baseBytes);
    final P baseParams = (P) PersistenceUtils.fromBinary(this.options.baseParamBytes);
    agg.setParameters(baseParams);
    return agg;
  }

  @Override
  public BinningAggregationOptions<P, T> getParameters() {
    return this.options;
  }

  @Override
  public void setParameters(final BinningAggregationOptions<P, T> parameters) {
    this.options = parameters;
  }

  @Override
  public byte[] resultToBinary(final Map<ByteArray, R> result) {
    final Aggregation<P, R, T> agg = this.getHelperAggregation();
    final Map<ByteArray, byte[]> mapped =
        result.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, e -> agg.resultToBinary(e.getValue())));
    final int totalDataSize =
        mapped.entrySet().stream().mapToInt(
            e -> (VarintUtils.unsignedIntByteLength(e.getKey().getBytes().length)
                + e.getKey().getBytes().length
                + VarintUtils.unsignedIntByteLength(e.getValue().length)
                + e.getValue().length)).reduce(0, Integer::sum);
    final ByteBuffer bb = ByteBuffer.allocate(totalDataSize);
    mapped.forEach((k, v) -> {
      VarintUtils.writeUnsignedInt(k.getBytes().length, bb);
      bb.put(k.getBytes());
      VarintUtils.writeUnsignedInt(v.length, bb);
      bb.put(v);
    });
    return bb.array();
  }

  @Override
  public Map<ByteArray, R> resultFromBinary(final byte[] binary) {
    final Aggregation<P, R, T> agg = this.getHelperAggregation();
    final ByteBuffer bb = ByteBuffer.wrap(binary);
    final Map<ByteArray, R> resultMap = new HashMap<>();
    while (bb.hasRemaining()) {
      final int keyLen = VarintUtils.readUnsignedInt(bb);
      final byte[] keyBytes = new byte[keyLen];
      bb.get(keyBytes);
      final ByteArray key = new ByteArray(keyBytes);

      final int valLen = VarintUtils.readUnsignedInt(bb);
      final byte[] valBytes = new byte[valLen];
      bb.get(valBytes);
      final R val = agg.resultFromBinary(valBytes);

      resultMap.put(key, val);
    }
    return resultMap;
  }

  private Aggregation<P, R, T> getHelperAggregation() {
    if (this.helperAggregation == null) {
      this.helperAggregation = this.instantiateBaseAggregation();
    }
    return this.helperAggregation;
  }
}
