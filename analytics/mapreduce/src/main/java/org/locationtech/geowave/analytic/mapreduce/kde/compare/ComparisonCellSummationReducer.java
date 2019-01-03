/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.kde.compare;

import org.apache.hadoop.io.LongWritable;
import org.locationtech.geowave.analytic.mapreduce.kde.CellSummationReducer;

public class ComparisonCellSummationReducer extends CellSummationReducer {

  @Override
  protected void collectStats(
      final LongWritable key,
      final double sum,
      final org.apache.hadoop.mapreduce.Reducer.Context context) {
    long positiveKey = key.get();
    boolean isWinter = false;
    if (positiveKey < 0) {
      positiveKey = -positiveKey - 1;
      isWinter = true;
    }

    final long level = (positiveKey % numLevels) + minLevel;

    context.getCounter(
        "Entries per level (" + (isWinter ? "winter" : "summer") + ")",
        "level " + Long.toString(level)).increment(1);
  }
}
