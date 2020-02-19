/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
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
import org.opengis.feature.simple.SimpleFeature;

/** Assumes two-dimensional simple feature without time dimensions. */
public class SimpleFeatureProjection implements Projection<SimpleFeature> {

  @Override
  public Geometry getProjection(final SimpleFeature anItem) {
    return (Geometry) anItem.getDefaultGeometry();
  }

  @Override
  public void initialize(final JobContext context, final Class<?> scope) throws IOException {}

  @Override
  public void setup(
      final PropertyManagement runTimeProperties,
      final Class<?> scope,
      final Configuration configuration) {}
}
