/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples.stats;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsDeleteCallback;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;
import org.locationtech.geowave.core.store.statistics.field.FieldStatisticType;
import com.beust.jcommander.Parameter;

public class WordCountStatistic extends FieldStatistic<WordCountStatistic.WordCountValue> {

  public static final FieldStatisticType<WordCountValue> STATS_TYPE =
      new FieldStatisticType<>("WORD_COUNT");

  private static final String WHITESPACE_REGEX = "\\s+";

  /**
   * Statistics support JCommander parameters so that they can be configured when adding the
   * statistic via the CLI. In this case, the minimum word length for the statistic would be
   * configurable via the `--minWordLength <length>` option when adding this statistic.
   */
  @Parameter(
      names = "--minWordLength",
      required = true,
      description = "The minimum word length to count.")
  private int minWordLength = 0;

  public WordCountStatistic() {
    super(STATS_TYPE);
  }

  public WordCountStatistic(final String typeName, final String fieldName) {
    super(STATS_TYPE, typeName, fieldName);
  }

  /**
   * Add a programmatic setter for min word length.
   */
  public void setMinWordLength(final int length) {
    this.minWordLength = length;
  }

  /**
   * Provides a description of the statistic that will be displayed in the CLI when describing
   * available statistics.
   */
  @Override
  public String getDescription() {
    return "Provides a count of all words of a string field.";
  }

  /**
   * Returns `true` for every class this statistic is compatible with. In our case, only `String`
   * types will be supported since we are doing a word count.
   */
  @Override
  public boolean isCompatibleWith(final Class<?> fieldClass) {
    return String.class.isAssignableFrom(fieldClass);
  }

  /**
   * Constructs an empty statistic value for this statistic. The state of the value should be as if
   * no entries have been ingested.
   */
  @Override
  public WordCountValue createEmpty() {
    return new WordCountValue(this);
  }

  /**
   * The `byteLength`, `writeBytes`, and `readBytes` functions only need to be overriden if you are
   * adding additional configuration parameters or need to store additional information needed for
   * the statistic to function properly. In this example, we have added a minimum word length
   * parameter, so we need to store that when the statistic is serialized and deserialized.
   */
  @Override
  protected int byteLength() {
    return super.byteLength() + Integer.BYTES;
  }

  @Override
  protected void writeBytes(ByteBuffer buffer) {
    super.writeBytes(buffer);
    buffer.putInt(minWordLength);
  }

  @Override
  protected void readBytes(ByteBuffer buffer) {
    super.readBytes(buffer);
    minWordLength = buffer.getInt();
  }

  /**
   * Every statistic has a corresponding statistic value. This class is responsible for determining
   * what happens when entries are ingested or deleted, as well as when two values need to be
   * merged. If a value can be updated on ingest, `StatisticsIngestCallback` should be implemented.
   * If the value can be updated on delete, `StatisticsDeleteCallback` should be implemented. Some
   * statistics, such as bounding box statistics cannot be updated on delete because there isn't
   * enough information to know if the bounding box should shrink when an entry is deleted. In that
   * case, only the ingest callback would be implemented.
   */
  public static class WordCountValue extends StatisticValue<Long> implements
      StatisticsIngestCallback,
      StatisticsDeleteCallback {
    private long count = 0;

    public WordCountValue() {
      this(null);
    }

    private WordCountValue(final WordCountStatistic statistic) {
      super(statistic);
    }

    public long getCount() {
      return count;
    }

    /**
     * Merge this value with another.
     */
    @Override
    public void merge(final Mergeable merge) {
      if ((merge != null) && (merge instanceof WordCountValue)) {
        final WordCountValue other = (WordCountValue) merge;
        count += other.count;
      }
    }

    /**
     * Get the field value from the adapter, and if it's not null, count the number of words that
     * exceed the minimum length and add it to the total.
     */
    @Override
    public <T> void entryIngested(
        final DataTypeAdapter<T> adapter,
        final T entry,
        final GeoWaveRow... rows) {
      final WordCountStatistic stat = ((WordCountStatistic) getStatistic());
      final Object o = adapter.getFieldValue(entry, stat.getFieldName());
      if (o == null) {
        return;
      }
      final String str = (String) o;
      final String[] split = str.split(WHITESPACE_REGEX);
      for (String word : split) {
        if (word.length() >= stat.minWordLength) {
          count++;
        }
      }
    }

    /**
     * Get the field value from the adapter, and if it's not null, count the number of words that
     * exceed the minimum length and subtract it from the total.
     */
    @Override
    public <T> void entryDeleted(
        final DataTypeAdapter<T> adapter,
        final T entry,
        final GeoWaveRow... rows) {
      final WordCountStatistic stat = ((WordCountStatistic) getStatistic());
      final Object o = adapter.getFieldValue(entry, stat.getFieldName());
      if (o == null) {
        return;
      }
      final String str = (String) o;
      final String[] split = str.split(WHITESPACE_REGEX);
      for (String word : split) {
        if (word.length() >= stat.minWordLength) {
          count++;
        }
      }
    }

    /**
     * Return the actual value of the statistic.
     */
    @Override
    public Long getValue() {
      return getCount();
    }

    /**
     * Serialize the statistic value to binary.
     */
    @Override
    public byte[] toBinary() {
      final ByteBuffer buffer = ByteBuffer.allocate(VarintUtils.unsignedLongByteLength(count));
      VarintUtils.writeUnsignedLong(count, buffer);
      return buffer.array();
    }

    /**
     * Deserialize the statistic value from binary.
     */
    @Override
    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buffer = ByteBuffer.wrap(bytes);
      count = VarintUtils.readUnsignedLong(buffer);
    }
  }
}

