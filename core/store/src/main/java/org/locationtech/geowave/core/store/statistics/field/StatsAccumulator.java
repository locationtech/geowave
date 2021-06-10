/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * Copyright (C) 2012 The Guava Authors
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.field;

// This is a copy from Guava, because HBase is still dependent on Guava 12 as a server-side library
// dependency and this was first introduced in Guava 20, this is basically a re-packaging of the
// Guava class to eliminate the Guava version incompatiblities for libraries such as HBase

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.primitives.Doubles.isFinite;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import java.util.Iterator;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import com.google.common.annotations.Beta;
import com.google.common.annotations.GwtIncompatible;

/**
 * A mutable object which accumulates double values and tracks some basic statistics over all the
 * values added so far. The values may be added singly or in groups. This class is not thread safe.
 *
 * @author Pete Gillin
 * @author Kevin Bourrillion
 * @since 20.0
 */
@Beta
@GwtIncompatible
public final class StatsAccumulator {

  // These fields must satisfy the requirements of Stats' constructor as well as those of the stat
  // methods of this class.
  private long count = 0;
  private double mean = 0.0; // any finite value will do, we only use it to multiply by zero for sum
  private double sumOfSquaresOfDeltas = 0.0;
  private double min = NaN; // any value will do
  private double max = NaN; // any value will do

  /** Adds the given value to the dataset. */
  public void add(final double value) {
    if (count == 0) {
      count = 1;
      mean = value;
      min = value;
      max = value;
      if (!isFinite(value)) {
        sumOfSquaresOfDeltas = NaN;
      }
    } else {
      count++;
      if (isFinite(value) && isFinite(mean)) {
        // Art of Computer Programming vol. 2, Knuth, 4.2.2, (15) and (16)
        final double delta = value - mean;
        mean += delta / count;
        sumOfSquaresOfDeltas += delta * (value - mean);
      } else {
        mean = calculateNewMeanNonFinite(mean, value);
        sumOfSquaresOfDeltas = NaN;
      }
      min = Math.min(min, value);
      max = Math.max(max, value);
    }
  }

  /**
   * Adds the given values to the dataset.
   *
   * @param values a series of values, which will be converted to {@code double} values (this may
   *        cause loss of precision)
   */
  public void addAll(final Iterable<? extends Number> values) {
    for (final Number value : values) {
      add(value.doubleValue());
    }
  }

  /**
   * Adds the given values to the dataset.
   *
   * @param values a series of values, which will be converted to {@code double} values (this may
   *        cause loss of precision)
   */
  public void addAll(final Iterator<? extends Number> values) {
    while (values.hasNext()) {
      add(values.next().doubleValue());
    }
  }

  /**
   * Adds the given values to the dataset.
   *
   * @param values a series of values
   */
  public void addAll(final double... values) {
    for (final double value : values) {
      add(value);
    }
  }

  /**
   * Adds the given values to the dataset.
   *
   * @param values a series of values
   */
  public void addAll(final int... values) {
    for (final int value : values) {
      add(value);
    }
  }

  /**
   * Adds the given values to the dataset.
   *
   * @param values a series of values, which will be converted to {@code double} values (this may
   *        cause loss of precision for longs of magnitude over 2^53 (slightly over 9e15))
   */
  public void addAll(final long... values) {
    for (final long value : values) {
      add(value);
    }
  }

  /**
   * Adds the given values to the dataset. The stream will be completely consumed by this method.
   *
   * @param values a series of values
   * @since 28.2
   */
  public void addAll(final DoubleStream values) {
    addAll(values.collect(StatsAccumulator::new, StatsAccumulator::add, StatsAccumulator::addAll));
  }

  /**
   * Adds the given values to the dataset. The stream will be completely consumed by this method.
   *
   * @param values a series of values
   * @since 28.2
   */
  public void addAll(final IntStream values) {
    addAll(values.collect(StatsAccumulator::new, StatsAccumulator::add, StatsAccumulator::addAll));
  }

  /**
   * Adds the given values to the dataset. The stream will be completely consumed by this method.
   *
   * @param values a series of values, which will be converted to {@code double} values (this may
   *        cause loss of precision for longs of magnitude over 2^53 (slightly over 9e15))
   * @since 28.2
   */
  public void addAll(final LongStream values) {
    addAll(values.collect(StatsAccumulator::new, StatsAccumulator::add, StatsAccumulator::addAll));
  }

  /**
   * Adds the given statistics to the dataset, as if the individual values used to compute the
   * statistics had been added directly.
   */
  public void addAll(final Stats values) {
    if (values.count() == 0) {
      return;
    }
    merge(values.count(), values.mean(), values.sumOfSquaresOfDeltas(), values.min(), values.max());
  }

  /**
   * Adds the given statistics to the dataset, as if the individual values used to compute the
   * statistics had been added directly.
   *
   * @since 28.2
   */
  public void addAll(final StatsAccumulator values) {
    if (values.count() == 0) {
      return;
    }
    merge(values.count(), values.mean(), values.sumOfSquaresOfDeltas(), values.min(), values.max());
  }

  private void merge(
      final long otherCount,
      final double otherMean,
      final double otherSumOfSquaresOfDeltas,
      final double otherMin,
      final double otherMax) {
    if (count == 0) {
      count = otherCount;
      mean = otherMean;
      sumOfSquaresOfDeltas = otherSumOfSquaresOfDeltas;
      min = otherMin;
      max = otherMax;
    } else {
      count += otherCount;
      if (isFinite(mean) && isFinite(otherMean)) {
        // This is a generalized version of the calculation in add(double) above.
        final double delta = otherMean - mean;
        mean += (delta * otherCount) / count;
        sumOfSquaresOfDeltas +=
            otherSumOfSquaresOfDeltas + (delta * (otherMean - mean) * otherCount);
      } else {
        mean = calculateNewMeanNonFinite(mean, otherMean);
        sumOfSquaresOfDeltas = NaN;
      }
      min = Math.min(min, otherMin);
      max = Math.max(max, otherMax);
    }
  }

  /** Returns an immutable snapshot of the current statistics. */
  public Stats snapshot() {
    return new Stats(count, mean, sumOfSquaresOfDeltas, min, max);
  }

  /** Returns the number of values. */
  public long count() {
    return count;
  }

  /**
   * Returns the <a href="http://en.wikipedia.org/wiki/Arithmetic_mean">arithmetic mean</a> of the
   * values. The count must be non-zero.
   *
   * <p>If these values are a sample drawn from a population, this is also an unbiased estimator of
   * the arithmetic mean of the population.
   *
   * <h3>Non-finite values</h3>
   *
   * <p>If the dataset contains {@link Double#NaN} then the result is {@link Double#NaN}. If it
   * contains both {@link Double#POSITIVE_INFINITY} and {@link Double#NEGATIVE_INFINITY} then the
   * result is {@link Double#NaN}. If it contains {@link Double#POSITIVE_INFINITY} and finite values
   * only or {@link Double#POSITIVE_INFINITY} only, the result is {@link Double#POSITIVE_INFINITY}.
   * If it contains {@link Double#NEGATIVE_INFINITY} and finite values only or
   * {@link Double#NEGATIVE_INFINITY} only, the result is {@link Double#NEGATIVE_INFINITY}.
   *
   * @throws IllegalStateException if the dataset is empty
   */
  public double mean() {
    checkState(count != 0);
    return mean;
  }

  /**
   * Returns the sum of the values.
   *
   * <h3>Non-finite values</h3>
   *
   * <p>If the dataset contains {@link Double#NaN} then the result is {@link Double#NaN}. If it
   * contains both {@link Double#POSITIVE_INFINITY} and {@link Double#NEGATIVE_INFINITY} then the
   * result is {@link Double#NaN}. If it contains {@link Double#POSITIVE_INFINITY} and finite values
   * only or {@link Double#POSITIVE_INFINITY} only, the result is {@link Double#POSITIVE_INFINITY}.
   * If it contains {@link Double#NEGATIVE_INFINITY} and finite values only or
   * {@link Double#NEGATIVE_INFINITY} only, the result is {@link Double#NEGATIVE_INFINITY}.
   */
  public final double sum() {
    return mean * count;
  }

  /**
   * Returns the <a href="http://en.wikipedia.org/wiki/Variance#Population_variance">population
   * variance</a> of the values. The count must be non-zero.
   *
   * <p>This is guaranteed to return zero if the dataset contains only exactly one finite value. It
   * is not guaranteed to return zero when the dataset consists of the same value multiple times,
   * due to numerical errors. However, it is guaranteed never to return a negative result.
   *
   * <h3>Non-finite values</h3>
   *
   * <p>If the dataset contains any non-finite values ({@link Double#POSITIVE_INFINITY},
   * {@link Double#NEGATIVE_INFINITY}, or {@link Double#NaN}) then the result is {@link Double#NaN}.
   *
   * @throws IllegalStateException if the dataset is empty
   */
  public final double populationVariance() {
    checkState(count != 0);
    if (isNaN(sumOfSquaresOfDeltas)) {
      return NaN;
    }
    if (count == 1) {
      return 0.0;
    }
    return ensureNonNegative(sumOfSquaresOfDeltas) / count;
  }

  /**
   * Returns the <a
   * href="http://en.wikipedia.org/wiki/Standard_deviation#Definition_of_population_values">
   * population standard deviation</a> of the values. The count must be non-zero.
   *
   * <p>This is guaranteed to return zero if the dataset contains only exactly one finite value. It
   * is not guaranteed to return zero when the dataset consists of the same value multiple times,
   * due to numerical errors. However, it is guaranteed never to return a negative result.
   *
   * <h3>Non-finite values</h3>
   *
   * <p>If the dataset contains any non-finite values ({@link Double#POSITIVE_INFINITY},
   * {@link Double#NEGATIVE_INFINITY}, or {@link Double#NaN}) then the result is {@link Double#NaN}.
   *
   * @throws IllegalStateException if the dataset is empty
   */
  public final double populationStandardDeviation() {
    return Math.sqrt(populationVariance());
  }

  /**
   * Returns the <a href="http://en.wikipedia.org/wiki/Variance#Sample_variance">unbiased sample
   * variance</a> of the values. If this dataset is a sample drawn from a population, this is an
   * unbiased estimator of the population variance of the population. The count must be greater than
   * one.
   *
   * <p>This is not guaranteed to return zero when the dataset consists of the same value multiple
   * times, due to numerical errors. However, it is guaranteed never to return a negative result.
   *
   * <h3>Non-finite values</h3>
   *
   * <p>If the dataset contains any non-finite values ({@link Double#POSITIVE_INFINITY},
   * {@link Double#NEGATIVE_INFINITY}, or {@link Double#NaN}) then the result is {@link Double#NaN}.
   *
   * @throws IllegalStateException if the dataset is empty or contains a single value
   */
  public final double sampleVariance() {
    checkState(count > 1);
    if (isNaN(sumOfSquaresOfDeltas)) {
      return NaN;
    }
    return ensureNonNegative(sumOfSquaresOfDeltas) / (count - 1);
  }

  /**
   * Returns the <a
   * href="http://en.wikipedia.org/wiki/Standard_deviation#Corrected_sample_standard_deviation">
   * corrected sample standard deviation</a> of the values. If this dataset is a sample drawn from a
   * population, this is an estimator of the population standard deviation of the population which
   * is less biased than {@link #populationStandardDeviation()} (the unbiased estimator depends on
   * the distribution). The count must be greater than one.
   *
   * <p>This is not guaranteed to return zero when the dataset consists of the same value multiple
   * times, due to numerical errors. However, it is guaranteed never to return a negative result.
   *
   * <h3>Non-finite values</h3>
   *
   * <p>If the dataset contains any non-finite values ({@link Double#POSITIVE_INFINITY},
   * {@link Double#NEGATIVE_INFINITY}, or {@link Double#NaN}) then the result is {@link Double#NaN}.
   *
   * @throws IllegalStateException if the dataset is empty or contains a single value
   */
  public final double sampleStandardDeviation() {
    return Math.sqrt(sampleVariance());
  }

  /**
   * Returns the lowest value in the dataset. The count must be non-zero.
   *
   * <h3>Non-finite values</h3>
   *
   * <p>If the dataset contains {@link Double#NaN} then the result is {@link Double#NaN}. If it
   * contains {@link Double#NEGATIVE_INFINITY} and not {@link Double#NaN} then the result is
   * {@link Double#NEGATIVE_INFINITY}. If it contains {@link Double#POSITIVE_INFINITY} and finite
   * values only then the result is the lowest finite value. If it contains
   * {@link Double#POSITIVE_INFINITY} only then the result is {@link Double#POSITIVE_INFINITY}.
   *
   * @throws IllegalStateException if the dataset is empty
   */
  public double min() {
    checkState(count != 0);
    return min;
  }

  /**
   * Returns the highest value in the dataset. The count must be non-zero.
   *
   * <h3>Non-finite values</h3>
   *
   * <p>If the dataset contains {@link Double#NaN} then the result is {@link Double#NaN}. If it
   * contains {@link Double#POSITIVE_INFINITY} and not {@link Double#NaN} then the result is
   * {@link Double#POSITIVE_INFINITY}. If it contains {@link Double#NEGATIVE_INFINITY} and finite
   * values only then the result is the highest finite value. If it contains
   * {@link Double#NEGATIVE_INFINITY} only then the result is {@link Double#NEGATIVE_INFINITY}.
   *
   * @throws IllegalStateException if the dataset is empty
   */
  public double max() {
    checkState(count != 0);
    return max;
  }

  double sumOfSquaresOfDeltas() {
    return sumOfSquaresOfDeltas;
  }

  /**
   * Calculates the new value for the accumulated mean when a value is added, in the case where at
   * least one of the previous mean and the value is non-finite.
   */
  static double calculateNewMeanNonFinite(final double previousMean, final double value) {
    /*
     * Desired behaviour is to match the results of applying the naive mean formula. In particular,
     * the update formula can subtract infinities in cases where the naive formula would add them.
     *
     * Consequently: 1. If the previous mean is finite and the new value is non-finite then the new
     * mean is that value (whether it is NaN or infinity). 2. If the new value is finite and the
     * previous mean is non-finite then the mean is unchanged (whether it is NaN or infinity). 3. If
     * both the previous mean and the new value are non-finite and... 3a. ...either or both is NaN
     * (so mean != value) then the new mean is NaN. 3b. ...they are both the same infinities (so
     * mean == value) then the mean is unchanged. 3c. ...they are different infinities (so mean !=
     * value) then the new mean is NaN.
     */
    if (isFinite(previousMean)) {
      // This is case 1.
      return value;
    } else if (isFinite(value) || (previousMean == value)) {
      // This is case 2. or 3b.
      return previousMean;
    } else {
      // This is case 3a. or 3c.
      return NaN;
    }
  }

  /** Returns its argument if it is non-negative, zero if it is negative. */
  static double ensureNonNegative(final double value) {
    checkArgument(!isNaN(value));
    return Math.max(value, 0.0);
  }
}
