/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.geotools.raster;

import org.locationtech.geowave.adapter.raster.adapter.merge.RasterTileMergeStrategy;
import org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;

public class NoDataMergeStrategyProvider implements RasterMergeStrategyProviderSpi {
  public static final String NAME = "no-data";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public RasterTileMergeStrategy<?> getStrategy() {
    return new NoDataMergeStrategy();
  }
}
