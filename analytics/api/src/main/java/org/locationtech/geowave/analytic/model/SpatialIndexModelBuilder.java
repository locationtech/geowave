/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.model;

import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

/** Builds an index model with longitude and latitude. */
public class SpatialIndexModelBuilder implements IndexModelBuilder {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  public CommonIndexModel buildModel() {
    return new SpatialDimensionalityTypeProvider().createIndex(
        new SpatialOptions()).getIndexModel();
  }
}
