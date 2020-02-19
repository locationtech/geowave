/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.kde;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CellSummationCombiner extends
    Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {

  @Override
  public void reduce(
      final LongWritable key,
      final Iterable<DoubleWritable> values,
      final Context context) throws IOException, InterruptedException {
    double s = 0.0;

    for (final DoubleWritable value : values) {
      s += value.get();
    }
    context.write(key, new DoubleWritable(s));
  }
}
