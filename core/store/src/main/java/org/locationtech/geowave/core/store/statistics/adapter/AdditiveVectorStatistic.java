/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.adapter;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

/**
 * Maintains a fixed layout of integer counts and real-valued sums per bin, merged by addition. What
 * each position means is up to the writer and is identified by a schema id and version.
 *
 * <p> Values are not derived from ingested entries. They are computed elsewhere and written with
 * {@code DataStore.incorporateStatisticValue}, so recalculating this statistic resets it. Because
 * the vector is additive, a correction is written as the negated difference.
 *
 * <p> Schema id 0 is reserved for the "unset" value, which is the identity of every merge, so a
 * statistic's schema id must be at least 1. Values of different schema ids are never added: the
 * larger schema id wins and the other value is dropped. Merging never throws, because HBase runs it
 * inside a memstore flush. The DataStore write path rejects a value of another schema id (see
 * {@link #validateValue}), so only data already stored with mixed schema ids, from before a schema
 * change, is merged this way.
 *
 * <p> Precision: sums are held as doubles in memory but stored as 32-bit floats, and are re-rounded
 * on every persisted merge (HBase flush and compaction, statistic compaction and
 * {@code mergeStats}). A quantity derived from them, such as a variance from a sum of squares, can
 * come out slightly negative, and a concentration from sums of cosines and sines slightly above 1.
 * Consumers must clamp them (variance >= 0, concentration <= 1).
 */
public class AdditiveVectorStatistic extends
    DataTypeStatistic<AdditiveVectorStatistic.AdditiveVectorValue> {
  public static final DataTypeStatisticType<AdditiveVectorValue> STATS_TYPE =
      new DataTypeStatisticType<>("ADDITIVE_VECTOR");
  private static final Logger LOGGER = LoggerFactory.getLogger(AdditiveVectorStatistic.class);

  @Parameter(
      names = "--schemaId",
      description = "Identifies the layout of the vector, at least 1 (0 is reserved for unset). Values of different schema ids are not merged: the larger schema id wins and the other value is dropped.",
      validateValueWith = SchemaIdValidator.class)
  private int schemaId = 1;

  @Parameter(
      names = "--schemaVersion",
      description = "The version of the layout. A later version may only append counts or sums.")
  private int schemaVersion = 1;

  @Parameter(
      names = "--countLength",
      description = "The number of integer counts in the vector.",
      validateValueWith = LengthValidator.class)
  private int countLength = 0;

  @Parameter(
      names = "--sumLength",
      description = "The number of real-valued sums in the vector.",
      validateValueWith = LengthValidator.class)
  private int sumLength = 0;

  public AdditiveVectorStatistic() {
    super(STATS_TYPE);
  }

  public AdditiveVectorStatistic(
      final String typeName,
      final int schemaId,
      final int schemaVersion,
      final int countLength,
      final int sumLength) {
    super(STATS_TYPE, typeName);
    validate(schemaId, countLength, sumLength);
    this.schemaId = schemaId;
    this.schemaVersion = schemaVersion;
    this.countLength = countLength;
    this.sumLength = sumLength;
  }

  private static void validate(final int schemaId, final int countLength, final int sumLength) {
    if (schemaId < 1) {
      throw new IllegalArgumentException("The schema id must be at least 1, got " + schemaId);
    }
    if (countLength < 0 || sumLength < 0) {
      throw new IllegalArgumentException(
          "The count and sum lengths must not be negative, got "
              + countLength
              + " and "
              + sumLength);
    }
  }

  public int getSchemaId() {
    return schemaId;
  }

  public int getSchemaVersion() {
    return schemaVersion;
  }

  public int getCountLength() {
    return countLength;
  }

  public int getSumLength() {
    return sumLength;
  }

  @Override
  public String getDescription() {
    return "Maintains a vector of counts and sums that is merged by addition, written from an external source.";
  }

  @Override
  public void validateValue(final AdditiveVectorValue value) {
    if (value.getSchemaId() != AdditiveVectorValue.UNSET_SCHEMA
        && value.getSchemaId() != schemaId) {
      throw new IllegalArgumentException(
          "The value has schema id "
              + value.getSchemaId()
              + " but the statistic has schema id "
              + schemaId);
    }
  }

  @Override
  public AdditiveVectorValue createEmpty() {
    return new AdditiveVectorValue(this, schemaId, schemaVersion, countLength, sumLength);
  }

  @Override
  protected int byteLength() {
    return super.byteLength()
        + VarintUtils.unsignedIntByteLength(schemaId)
        + VarintUtils.unsignedIntByteLength(schemaVersion)
        + VarintUtils.unsignedIntByteLength(countLength)
        + VarintUtils.unsignedIntByteLength(sumLength);
  }

  @Override
  protected void writeBytes(final ByteBuffer buffer) {
    super.writeBytes(buffer);
    VarintUtils.writeUnsignedInt(schemaId, buffer);
    VarintUtils.writeUnsignedInt(schemaVersion, buffer);
    VarintUtils.writeUnsignedInt(countLength, buffer);
    VarintUtils.writeUnsignedInt(sumLength, buffer);
  }

  @Override
  protected void readBytes(final ByteBuffer buffer) {
    super.readBytes(buffer);
    schemaId = VarintUtils.readUnsignedInt(buffer);
    schemaVersion = VarintUtils.readUnsignedInt(buffer);
    countLength = VarintUtils.readUnsignedInt(buffer);
    sumLength = VarintUtils.readUnsignedInt(buffer);
    validate(schemaId, countLength, sumLength);
  }

  public static class SchemaIdValidator implements IValueValidator<Integer> {
    @Override
    public void validate(final String name, final Integer value) throws ParameterException {
      if (value < 1) {
        throw new ParameterException(name + " must be at least 1 (0 is reserved for unset).");
      }
    }
  }

  public static class LengthValidator implements IValueValidator<Integer> {
    @Override
    public void validate(final String name, final Integer value) throws ParameterException {
      if (value < 0) {
        throw new ParameterException(name + " must not be negative.");
      }
    }
  }

  /**
   * Counts are serialized as zigzag varints and sums as 32-bit floats, each block either densely or
   * as (index gap, value) pairs, whichever is smaller. Sums are held as doubles in memory, but
   * every persisted merge reads them back as floats and rounds again, so derived quantities (a
   * variance from a sum of squares, a concentration from cosine and sine sums) must be clamped by
   * the consumer. See {@link AdditiveVectorStatistic}.
   *
   * <p> Schema id 0 is the unset value, which only the no-arg and statistic-only constructors
   * produce. Merging never throws: values of different schema ids are not added, the larger id wins
   * and the other is dropped. The DataStore write path rejects a value whose schema id differs from
   * its statistic's, so this only applies to data already stored with mixed schema ids.
   */
  public static class AdditiveVectorValue extends StatisticValue<AdditiveVectorValue> {
    static final int UNSET_SCHEMA = 0;

    private int schemaId;
    private int schemaVersion;
    private long[] counts;
    private double[] sums;

    public AdditiveVectorValue() {
      this(null);
    }

    public AdditiveVectorValue(final Statistic<?> statistic) {
      super(statistic);
      schemaId = UNSET_SCHEMA;
      counts = new long[0];
      sums = new double[0];
    }

    public AdditiveVectorValue(
        final Statistic<?> statistic,
        final int schemaId,
        final int schemaVersion,
        final int countLength,
        final int sumLength) {
      super(statistic);
      validate(schemaId, countLength, sumLength);
      this.schemaId = schemaId;
      this.schemaVersion = schemaVersion;
      counts = new long[countLength];
      sums = new double[sumLength];
    }

    public int getSchemaId() {
      return schemaId;
    }

    public int getSchemaVersion() {
      return schemaVersion;
    }

    public int getCountLength() {
      return counts.length;
    }

    public int getSumLength() {
      return sums.length;
    }

    public long getCount(final int index) {
      return index < counts.length ? counts[index] : 0L;
    }

    public double getSum(final int index) {
      return index < sums.length ? sums[index] : 0.0;
    }

    public long[] getCounts() {
      return counts.clone();
    }

    public double[] getSums() {
      return sums.clone();
    }

    public void addCount(final int index, final long delta) {
      counts[index] += delta;
    }

    public void addSum(final int index, final double delta) {
      sums[index] += delta;
    }

    public boolean isEmpty() {
      return Arrays.stream(counts).allMatch(c -> c == 0L)
          && Arrays.stream(sums).allMatch(s -> s == 0.0);
    }

    public void add(final AdditiveVectorValue other) {
      combine(other, 1);
    }

    public void subtract(final AdditiveVectorValue other) {
      combine(other, -1);
    }

    private void combine(final AdditiveVectorValue other, final int sign) {
      if (other.schemaId == UNSET_SCHEMA) {
        return;
      }
      if (schemaId != other.schemaId) {
        if (schemaId > other.schemaId) {
          LOGGER.warn(
              "Dropping an additive vector of schema {}, keeping schema {}",
              other.schemaId,
              schemaId);
          return;
        }
        if (schemaId != UNSET_SCHEMA) {
          LOGGER.warn(
              "Dropping an additive vector of schema {}, keeping schema {}",
              schemaId,
              other.schemaId);
        }
        schemaId = other.schemaId;
        schemaVersion = 0;
        counts = new long[0];
        sums = new double[0];
      }
      // later schema versions only append, so the shorter vector is zero-extended
      schemaVersion = Math.max(schemaVersion, other.schemaVersion);
      if (other.counts.length > counts.length) {
        counts = Arrays.copyOf(counts, other.counts.length);
      }
      if (other.sums.length > sums.length) {
        sums = Arrays.copyOf(sums, other.sums.length);
      }
      for (int i = 0; i < other.counts.length; i++) {
        counts[i] += sign * other.counts[i];
      }
      for (int i = 0; i < other.sums.length; i++) {
        sums[i] += sign * other.sums[i];
      }
    }

    @Override
    public void merge(final Mergeable merge) {
      if (merge instanceof AdditiveVectorValue) {
        add((AdditiveVectorValue) merge);
      }
    }

    @Override
    public AdditiveVectorValue getValue() {
      return this;
    }

    @Override
    public byte[] toBinary() {
      int nonZeroCounts = 0;
      int sparseCountBytes = 0;
      int denseCountBytes = 0;
      int previous = -1;
      for (int i = 0; i < counts.length; i++) {
        final int valueBytes = VarintUtils.signedLongByteLength(counts[i]);
        denseCountBytes += valueBytes;
        if (counts[i] != 0L) {
          nonZeroCounts++;
          sparseCountBytes += VarintUtils.unsignedIntByteLength(i - previous - 1) + valueBytes;
          previous = i;
        }
      }
      int nonZeroSums = 0;
      int sparseSumBytes = 0;
      previous = -1;
      for (int i = 0; i < sums.length; i++) {
        if ((float) sums[i] != 0f) {
          nonZeroSums++;
          sparseSumBytes += VarintUtils.unsignedIntByteLength(i - previous - 1) + Float.BYTES;
          previous = i;
        }
      }
      final boolean denseCounts = denseCountBytes < sparseCountBytes;
      final boolean denseSums = (sums.length * Float.BYTES) < sparseSumBytes;
      final int countHeader = denseCounts ? 1 : nonZeroCounts << 1;
      final int sumHeader = denseSums ? 1 : nonZeroSums << 1;
      final ByteBuffer buffer =
          ByteBuffer.allocate(
              VarintUtils.unsignedIntByteLength(schemaId)
                  + VarintUtils.unsignedIntByteLength(schemaVersion)
                  + VarintUtils.unsignedIntByteLength(counts.length)
                  + VarintUtils.unsignedIntByteLength(sums.length)
                  + VarintUtils.unsignedIntByteLength(countHeader)
                  + (denseCounts ? denseCountBytes : sparseCountBytes)
                  + VarintUtils.unsignedIntByteLength(sumHeader)
                  + (denseSums ? sums.length * Float.BYTES : sparseSumBytes));
      VarintUtils.writeUnsignedInt(schemaId, buffer);
      VarintUtils.writeUnsignedInt(schemaVersion, buffer);
      VarintUtils.writeUnsignedInt(counts.length, buffer);
      VarintUtils.writeUnsignedInt(sums.length, buffer);
      VarintUtils.writeUnsignedInt(countHeader, buffer);
      previous = -1;
      for (int i = 0; i < counts.length; i++) {
        if (denseCounts) {
          VarintUtils.writeSignedLong(counts[i], buffer);
        } else if (counts[i] != 0L) {
          VarintUtils.writeUnsignedInt(i - previous - 1, buffer);
          VarintUtils.writeSignedLong(counts[i], buffer);
          previous = i;
        }
      }
      VarintUtils.writeUnsignedInt(sumHeader, buffer);
      previous = -1;
      for (int i = 0; i < sums.length; i++) {
        if (denseSums) {
          buffer.putFloat((float) sums[i]);
        } else if ((float) sums[i] != 0f) {
          VarintUtils.writeUnsignedInt(i - previous - 1, buffer);
          buffer.putFloat((float) sums[i]);
          previous = i;
        }
      }
      return buffer.array();
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buffer = ByteBuffer.wrap(bytes);
      schemaId = VarintUtils.readUnsignedInt(buffer);
      schemaVersion = VarintUtils.readUnsignedInt(buffer);
      counts = new long[VarintUtils.readUnsignedInt(buffer)];
      sums = new double[VarintUtils.readUnsignedInt(buffer)];
      final int countHeader = VarintUtils.readUnsignedInt(buffer);
      if ((countHeader & 1) == 1) {
        for (int i = 0; i < counts.length; i++) {
          counts[i] = VarintUtils.readSignedLong(buffer);
        }
      } else {
        int index = -1;
        for (int n = countHeader >>> 1; n > 0; n--) {
          index += VarintUtils.readUnsignedInt(buffer) + 1;
          counts[index] = VarintUtils.readSignedLong(buffer);
        }
      }
      final int sumHeader = VarintUtils.readUnsignedInt(buffer);
      if ((sumHeader & 1) == 1) {
        for (int i = 0; i < sums.length; i++) {
          sums[i] = buffer.getFloat();
        }
      } else {
        int index = -1;
        for (int n = sumHeader >>> 1; n > 0; n--) {
          index += VarintUtils.readUnsignedInt(buffer) + 1;
          sums[index] = buffer.getFloat();
        }
      }
    }

    @Override
    public String toString() {
      return "schema "
          + schemaId
          + " v"
          + schemaVersion
          + " counts="
          + Arrays.toString(counts)
          + " sums="
          + Arrays.toString(sums);
    }
  }
}
