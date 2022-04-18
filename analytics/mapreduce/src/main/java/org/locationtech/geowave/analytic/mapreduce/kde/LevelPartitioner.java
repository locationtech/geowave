/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.kde;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public abstract class LevelPartitioner<K> extends Partitioner<K, LongWritable> {
  @Override
  public int getPartition(final K key, final LongWritable value, final int numReduceTasks) {
    return getPartition(value.get(), numReduceTasks);
  }

  protected int getPartition(final long positiveCellId, final int numReduceTasks) {
    return (int) (positiveCellId % numReduceTasks);
  }
}
