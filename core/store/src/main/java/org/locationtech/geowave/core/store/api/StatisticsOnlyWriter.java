/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import java.io.Closeable;
import java.io.Flushable;

/**
 * A writer that computes and stores statistics from entries without persisting the entries
 * themselves. This is useful for maintaining aggregate statistics from streaming data or external
 * sources without the overhead of storing all raw data.
 *
 * <p>Usage is identical to a regular {@link Writer}, but entries are only used to compute statistics
 * and are not stored:
 *
 * <pre>
 * {@code
 * // Register statistics first
 * dataStore.addEmptyStatistic(speedStats, accelerationStats);
 *
 * // Create a statistics-only writer
 * try (StatisticsOnlyWriter<Track> writer = dataStore.createStatisticsOnlyWriter("Track")) {
 *   for (Track track : streamingTracks) {
 *     writer.write(track);  // Computes statistics, does NOT store track
 *   }
 * }
 *
 * // Query statistics (no data was stored)
 * Stats stats = dataStore.getStatisticValue(speedStats);
 * }
 * </pre>
 *
 * <p>The writer automatically finds all statistics registered for the type, computes statistic
 * values from each entry, applies the statistic's binning strategy, and incorporates the values into
 * the appropriate bins, all without storing the entries themselves.
 *
 * @param <T> The type of entries to compute statistics from
 */
public interface StatisticsOnlyWriter<T> extends Closeable, Flushable {

  /**
   * Compute and incorporate statistics from an entry without storing the entry itself. This method
   * will update all statistics that have been registered for this type.
   *
   * @param entry the entry to compute statistics from (will not be stored)
   */
  void write(T entry);

  /**
   * Compute and incorporate statistics from multiple entries without storing the entries themselves.
   * This is a convenience method equivalent to calling {@link #write(Object)} for each entry.
   *
   * @param entries the entries to compute statistics from (will not be stored)
   */
  default void write(final Iterable<T> entries) {
    for (final T entry : entries) {
      write(entry);
    }
  }

  /**
   * Flush any buffered statistics to the underlying store. This ensures all computed statistics are
   * persisted.
   */
  @Override
  void flush();

  /**
   * Close the writer and flush any remaining statistics. After closing, the writer cannot be used
   * again.
   */
  @Override
  void close();
}
