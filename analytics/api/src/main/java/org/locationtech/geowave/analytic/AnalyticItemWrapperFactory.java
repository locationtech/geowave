/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic;

import java.io.IOException;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.jts.geom.Coordinate;
import org.slf4j.Logger;

/**
 * Create an analytic item wrapper for the provided item.
 *
 * @param <T> the type of the item to wrap
 */
public interface AnalyticItemWrapperFactory<T> {
  /**
   * Wrap the item.
   */
  public AnalyticItemWrapper<T> create(T item);

  /**
   * Creates a new item based on the old item with new coordinates and dimension values
   */
  public AnalyticItemWrapper<T> createNextItem(
      final T feature,
      final String groupID,
      final Coordinate coordinate,
      final String[] extraNames,
      final double[] extraValues);

  public void initialize(final JobContext context, Class<?> scope, Logger logger)
      throws IOException;
}
