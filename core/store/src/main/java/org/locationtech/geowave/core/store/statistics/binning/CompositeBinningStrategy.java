/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.binning;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import com.google.common.collect.Lists;

/**
 * Statistic binning strategy that combines two or more other binning strategies.
 *
 * <p> A composite bin is the child bins concatenated, followed by their lengths, so leading child
 * bins form a key prefix. An Object[] constraint with one entry per child is combined from the
 * children's constraints: leading exact constraints multiply out, the first child that is not exact
 * may be constrained to all bins, prefixes or ranges (which become prefixes or ranges under each
 * leading combination), and every child after it must be constrained to all bins. Nothing separates
 * one child bin from the next, so those prefixes and ranges need every exact child before them, and
 * a child constrained by prefix or range, to have a fixed bin length (see
 * {@link StatisticBinningStrategy#getFixedBinLength()}); otherwise the constraint is refused.
 */
public class CompositeBinningStrategy implements StatisticBinningStrategy {

  public static final String NAME = "COMPOSITE";
  public static final byte[] WILDCARD_BYTES = new byte[0];

  private StatisticBinningStrategy[] childBinningStrategies;

  public CompositeBinningStrategy() {
    childBinningStrategies = new StatisticBinningStrategy[0];
  }

  public CompositeBinningStrategy(final StatisticBinningStrategy... childBinningStrategies) {
    this.childBinningStrategies = childBinningStrategies;
  }

  @Override
  public String getStrategyName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return "Bin the statistic using multiple strategies.";
  }

  @Override
  public <T> ByteArray[] getBins(
      final DataTypeAdapter<T> adapter,
      final T entry,
      final GeoWaveRow... rows) {
    final ByteArray[][] perStrategyBins =
        Arrays.stream(childBinningStrategies).map(s -> s.getBins(adapter, entry, rows)).toArray(
            ByteArray[][]::new);
    return getAllCombinations(perStrategyBins);
  }

  @Override
  public String getDefaultTag() {
    return Arrays.stream(childBinningStrategies).map(s -> s.getDefaultTag()).collect(
        Collectors.joining("|"));
  }

  @Override
  public void addFieldsUsed(final Set<String> fieldsUsed) {
    for (final StatisticBinningStrategy child : childBinningStrategies) {
      child.addFieldsUsed(fieldsUsed);
    }
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(Lists.newArrayList(childBinningStrategies));
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final List<Persistable> strategies = PersistenceUtils.fromBinaryAsList(bytes);
    childBinningStrategies = strategies.toArray(new StatisticBinningStrategy[strategies.size()]);
  }

  @Override
  public String binToString(final ByteArray bin) {
    if (bin == null || bin.getBytes() == null || bin.getBytes().length == 0) {
      return "None";
    }
    final ByteBuffer buffer = ByteBuffer.wrap(bin.getBytes());
    buffer.position(buffer.limit() - 1);
    final int[] byteLengths =
        Arrays.stream(childBinningStrategies).mapToInt(
            s -> VarintUtils.readUnsignedIntReversed(buffer)).toArray();
    buffer.rewind();
    final StringBuffer strVal = new StringBuffer();
    for (int i = 0; i < childBinningStrategies.length; i++) {
      if (i != 0) {
        strVal.append("|");
      }
      final byte[] subBin = new byte[byteLengths[i]];
      buffer.get(subBin);
      strVal.append(childBinningStrategies[i].binToString(new ByteArray(subBin)));
    }
    return strVal.toString();
  }

  public Pair<StatisticBinningStrategy, ByteArray>[] getSubBins(final ByteArray bin) {
    final ByteBuffer buffer = ByteBuffer.wrap(bin.getBytes());
    buffer.position(buffer.limit() - 1);
    final int[] byteLengths =
        Arrays.stream(childBinningStrategies).mapToInt(
            s -> VarintUtils.readUnsignedIntReversed(buffer)).toArray();
    buffer.rewind();
    @SuppressWarnings("unchecked")
    final Pair<StatisticBinningStrategy, ByteArray>[] retVal =
        new Pair[childBinningStrategies.length];
    for (int i = 0; i < childBinningStrategies.length; i++) {
      final byte[] subBin = new byte[byteLengths[i]];
      buffer.get(subBin);
      retVal[i] = Pair.of(childBinningStrategies[i], new ByteArray(subBin));
    }
    return retVal;
  }

  public boolean binMatches(
      final Class<? extends StatisticBinningStrategy> binningStrategyClass,
      final ByteArray bin,
      final ByteArray subBin) {
    // this logic only seems to be valid if the child binning strategy classes are different
    final ByteBuffer buffer = ByteBuffer.wrap(bin.getBytes());
    // first look to see if the strategy is directly assignable and at which position
    final OptionalInt directlyAssignable =
        IntStream.range(0, childBinningStrategies.length).filter(
            i -> binningStrategyClass.isAssignableFrom(
                childBinningStrategies[i].getClass())).findFirst();
    if (directlyAssignable.isPresent()) {
      return Arrays.equals(
          getSubBinAtIndex(directlyAssignable.getAsInt(), buffer),
          subBin.getBytes());
    }
    final OptionalInt composite =
        IntStream.range(0, childBinningStrategies.length).filter(
            i -> (childBinningStrategies[i] instanceof CompositeBinningStrategy)
                && ((CompositeBinningStrategy) childBinningStrategies[i]).usesStrategy(
                    binningStrategyClass)).findFirst();
    if (composite.isPresent()) {
      // get the subBin from the buffer at this position
      return ((CompositeBinningStrategy) childBinningStrategies[composite.getAsInt()]).binMatches(
          binningStrategyClass,
          new ByteArray(getSubBinAtIndex(composite.getAsInt(), buffer)),
          subBin);
    }
    return false;
  }

  private static byte[] getSubBinAtIndex(final int index, final ByteBuffer buffer) {
    // get the subBin from the buffer at this position
    buffer.position(buffer.limit() - 1);
    final int skipBytes =
        IntStream.range(0, index).map(i -> VarintUtils.readUnsignedIntReversed(buffer)).sum();
    final byte[] subBin = new byte[VarintUtils.readUnsignedIntReversed(buffer)];
    buffer.position(skipBytes);
    buffer.get(subBin);
    return subBin;
  }

  public boolean usesStrategy(
      final Class<? extends StatisticBinningStrategy> binningStrategyClass) {
    return Arrays.stream(childBinningStrategies).anyMatch(
        s -> binningStrategyClass.isAssignableFrom(s.getClass())
            || ((s instanceof CompositeBinningStrategy)
                && ((CompositeBinningStrategy) s).usesStrategy(binningStrategyClass)));
  }

  public boolean isOfType(final Class<?>... strategyClasses) {
    if (strategyClasses.length == childBinningStrategies.length) {
      return IntStream.range(0, strategyClasses.length).allMatch(
          i -> strategyClasses[i].isAssignableFrom(childBinningStrategies[i].getClass()));
    }
    return false;
  }

  public static ByteArray getBin(final ByteArray... bins) {
    final int byteLength =
        Arrays.stream(bins).map(ByteArray::getBytes).mapToInt(
            b -> b.length + VarintUtils.unsignedIntByteLength(b.length)).sum();
    final ByteBuffer bytes = ByteBuffer.allocate(byteLength);
    Arrays.stream(bins).map(ByteArray::getBytes).forEach(b -> {
      bytes.put(b);
    });
    // write the lengths at the back for deserialization purposes only (and so prefix scans don't
    // need to account for this)

    // also we want to iterate in reverse order so this reverses the order
    final Deque<ByteArray> output =
        Arrays.stream(bins).collect(
            Collector.of(ArrayDeque::new, (deq, t) -> deq.addFirst(t), (d1, d2) -> {
              d2.addAll(d1);
              return d2;
            }));
    output.stream().map(ByteArray::getBytes).forEach(b -> {
      VarintUtils.writeUnsignedIntReversed(b.length, bytes);
    });
    return new ByteArray(bytes.array());
  }

  @Override
  public ByteArrayConstraints constraints(final Object constraint) {
    if ((constraint != null) && (constraint instanceof Object[])) {
      return constraints((Object[]) constraint, childBinningStrategies);
    }
    return StatisticBinningStrategy.super.constraints(constraint);
  }

  private ByteArrayConstraints constraints(
      final Object[] constraints,
      final StatisticBinningStrategy[] binningStrategies) {
    // this will handle merging bins together per constraint-binningStrategy pair
    if (constraints.length == binningStrategies.length) {
      final List<ByteArrayConstraints> perStrategyConstraints =
          IntStream.range(0, constraints.length).mapToObj(
              i -> binningStrategies[i].constraints(constraints[i])).collect(Collectors.toList());
      return concat(perStrategyConstraints);
    }
    // if there's not the same number of constraints as binning strategies, use default logic
    return StatisticBinningStrategy.super.constraints(constraints);
  }

  private ByteArrayConstraints concat(final List<ByteArrayConstraints> perStrategyConstraints) {
    return BinningStrategyUtils.combineConstraints(perStrategyConstraints, new Layout());
  }

  @Override
  public OptionalInt getFixedBinLength() {
    int length = 0;
    for (final StatisticBinningStrategy child : childBinningStrategies) {
      final OptionalInt childLength = child.getFixedBinLength();
      if (childLength.isEmpty()) {
        return OptionalInt.empty();
      }
      length += childLength.getAsInt() + VarintUtils.unsignedIntByteLength(childLength.getAsInt());
    }
    return OptionalInt.of(length);
  }

  private class Layout implements BinningStrategyUtils.ComponentLayout {
    @Override
    public ByteArray getBin(final ByteArray[] componentBins) {
      return CompositeBinningStrategy.getBin(componentBins);
    }

    @Override
    public ByteArray getPrefix(final ByteArray[] leadingBins, final ByteArray partialBin) {
      return BinningStrategyUtils.concatenate(ArrayUtils.add(leadingBins, partialBin));
    }

    @Override
    public ByteArray getExactPrefix(final ByteArray[] leadingBins, final ByteArray bin) {
      // the bin has its child's fixed length, so no longer child bin can start with it
      return getPrefix(leadingBins, bin);
    }

    @Override
    public void checkDelimited(final int component, final ByteArrayConstraints constraints) {
      final OptionalInt length = childBinningStrategies[component].getFixedBinLength();
      if (length.isEmpty()) {
        // the next child's bytes or the bin lengths follow a child bin with nothing in between
        throw new IllegalArgumentException(
            "Cannot use prefix, range or 'all bins' constraints on or after "
                + describe(component)
                + " because its bins do not have a fixed length; constrain every binning strategy to exact bins, or all of them to all bins");
      }
      BinningStrategyUtils.checkFixedLength(length.getAsInt(), constraints, describe(component));
    }

    @Override
    public String describe(final int component) {
      return "binning strategy "
          + component
          + " ("
          + childBinningStrategies[component].getStrategyName()
          + ")";
    }
  }

  private static ByteArray[] getAllCombinations(final ByteArray[][] perStrategyBins) {
    return BinningStrategyUtils.getAllCombinations(
        perStrategyBins,
        CompositeBinningStrategy::getBin);
  }


}
