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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.jts.geom.Geometry;

/**
 * Project a n-dimensional item into a two-dimensional polygon for convex hull construction.
 *
 * @param <T>
 */
public interface Projection<T> {
  public Geometry getProjection(T anItem);

  public void initialize(JobContext context, Class<?> scope) throws IOException;

  public void setup(
      PropertyManagement runTimeProperties,
      Class<?> scope,
      Configuration configuration);
}
