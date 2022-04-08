/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.sample.function;

import java.io.IOException;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;

/**
 * Used to rank an object for selection in the sample set. The top K highest ranked objects are
 * sampled. Rank is between 0.0 and 1.0 inclusive.
 */
public interface SamplingRankFunction<T> {
  public void initialize(final JobContext context, Class<?> scope, Logger logger)
      throws IOException;

  public double rank(final int sampleSize, T value);
}
