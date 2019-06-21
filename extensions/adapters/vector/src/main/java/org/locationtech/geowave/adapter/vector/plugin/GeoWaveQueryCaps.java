/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin;

import org.geotools.data.QueryCapabilities;
import org.opengis.filter.sort.SortBy;

/** A definition of the Query capabilities provided to GeoTools by the GeoWave data store. */
public class GeoWaveQueryCaps extends QueryCapabilities {

  public GeoWaveQueryCaps() {}

  // TODO implement sorting...
  @Override
  public boolean supportsSorting(final SortBy[] sortAttributes) {
    // called for every WFS-T operation. Without sorting requests, the
    // argument is empty or null
    // returning false fails the operation, disabling any capability of
    // writing.
    return (sortAttributes == null) || (sortAttributes.length == 0);
  }
}
