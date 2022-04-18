/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.field;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * Applies a bloom filter to field values useful for quickly determining set membership. False
 * positives are possible but false negatives are not possible. In other words, a value can be
 * determined to be possibly in the set or definitely not in the set.
 */
public class BloomFilterStatistic extends FieldStatistic<BloomFilterStatistic.BloomFilterValue> {
  private static Logger LOGGER = LoggerFactory.getLogger(BloomFilterStatistic.class);
  @Parameter(
      names = "--expectedInsertions",
      description = "The number of expected insertions, used for appropriate sizing of bloom filter.")
  private long expectedInsertions = 10000;

  @Parameter(
      names = "--desiredFpp",
      description = "The desired False Positive Probability, directly related to the expected number of insertions. Higher FPP results in more compact Bloom Filter and lower FPP results in more accuracy.")
  private double desiredFalsePositiveProbability = 0.03;

  public static final FieldStatisticType<BloomFilterValue> STATS_TYPE =
      new FieldStatisticType<>("BLOOM_FILTER");

  public BloomFilterStatistic() {
    super(STATS_TYPE);
  }

  public BloomFilterStatistic(final String typeName, final String fieldName) {
    super(STATS_TYPE, typeName, fieldName);
  }

  public void setExpectedInsertions(final long expectedInsertions) {
    this.expectedInsertions = expectedInsertions;
  }

  public long getExpectedInsertions() {
    return this.expectedInsertions;
  }

  public void setDesiredFalsePositiveProbability(final double desiredFalsePositiveProbability) {
    this.desiredFalsePositiveProbability = desiredFalsePositiveProbability;
  }

  public double getDesiredFalsePositiveProbability() {
    return this.desiredFalsePositiveProbability;
  }

  @Override
  public String getDescription() {
    return "Provides a bloom filter used for probabilistically determining set membership.";
  }

  @Override
  public boolean isCompatibleWith(final Class<?> fieldClass) {
    return true;
  }

  @Override
  public BloomFilterValue createEmpty() {
    return new BloomFilterValue(this);
  }

  @Override
  protected int byteLength() {
    return super.byteLength()
        + VarintUtils.unsignedLongByteLength(expectedInsertions)
        + Double.BYTES;
  }

  @Override
  protected void writeBytes(ByteBuffer buffer) {
    super.writeBytes(buffer);
    VarintUtils.writeUnsignedLong(expectedInsertions, buffer);
    buffer.putDouble(desiredFalsePositiveProbability);
  }

  @Override
  protected void readBytes(ByteBuffer buffer) {
    super.readBytes(buffer);
    expectedInsertions = VarintUtils.readUnsignedLong(buffer);
    desiredFalsePositiveProbability = buffer.getDouble();
  }

  public static class BloomFilterValue extends StatisticValue<BloomFilter<CharSequence>> implements
      StatisticsIngestCallback {
    private BloomFilter<CharSequence> bloomFilter;

    public BloomFilterValue() {
      this(null);
    }

    private BloomFilterValue(final BloomFilterStatistic statistic) {
      super(statistic);
      if (statistic == null) {
        bloomFilter = null;
      } else {
        bloomFilter =
            BloomFilter.create(
                Funnels.unencodedCharsFunnel(),
                statistic.expectedInsertions,
                statistic.desiredFalsePositiveProbability);
      }
    }

    @Override
    public void merge(final Mergeable merge) {
      if ((merge != null) && (merge instanceof BloomFilterValue)) {
        final BloomFilterValue other = (BloomFilterValue) merge;
        if (bloomFilter == null) {
          bloomFilter = other.bloomFilter;
        } else if ((other.bloomFilter != null) && bloomFilter.isCompatible(other.bloomFilter)) {
          bloomFilter.putAll(other.bloomFilter);
        }
      }
    }

    @Override
    public <T> void entryIngested(
        final DataTypeAdapter<T> adapter,
        final T entry,
        final GeoWaveRow... rows) {
      final Object o =
          adapter.getFieldValue(entry, ((BloomFilterStatistic) getStatistic()).getFieldName());
      if (o == null) {
        return;
      }
      bloomFilter.put(o.toString());
    }

    @Override
    public BloomFilter<CharSequence> getValue() {
      return bloomFilter;
    }

    @Override
    public byte[] toBinary() {
      try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        bloomFilter.writeTo(baos);
        baos.flush();
        return baos.toByteArray();
      } catch (final IOException e) {
        LOGGER.warn("Unable to write bloom filter", e);
      }
      return new byte[0];
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      if (bytes.length > 0) {
        try {
          bloomFilter =
              BloomFilter.readFrom(new ByteArrayInputStream(bytes), Funnels.unencodedCharsFunnel());
        } catch (final IOException e) {
          LOGGER.error("Unable to read Bloom Filter", e);
        }
      }
    }
  }
}
