/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.resize;

import java.io.IOException;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.locationtech.geowave.mapreduce.GeoWaveReducer;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.opengis.coverage.grid.GridCoverage;

public class RasterTileResizeCombiner extends GeoWaveReducer {
  private RasterTileResizeHelper helper;

  @Override
  protected void reduceNativeValues(
      final GeoWaveInputKey key,
      final Iterable<Object> values,
      final ReduceContext<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, Object> context)
      throws IOException, InterruptedException {
    final GridCoverage mergedCoverage = helper.getMergedCoverage(key, values);
    if (mergedCoverage != null) {
      context.write(key, mergedCoverage);
    }
  }

  @Override
  protected void setup(
      final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context)
      throws IOException, InterruptedException {
    super.setup(context);
    helper = new RasterTileResizeHelper(context);
  }
}
