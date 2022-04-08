/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.landsat8;

import org.geotools.coverage.grid.GridCoverage2D;
import org.opengis.feature.simple.SimpleFeature;

public interface Landsat8BandConverterSpi {
  public String getName();

  public GridCoverage2D convert(
      final String coverageName,
      final GridCoverage2D originalBandData,
      final SimpleFeature bandMetadata);
}
