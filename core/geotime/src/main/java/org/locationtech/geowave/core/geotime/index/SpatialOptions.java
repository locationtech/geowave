/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.index;

import com.beust.jcommander.Parameter;

public class SpatialOptions extends CommonSpatialOptions {
  @Parameter(
      names = {"--storeTime"},
      required = false,
      description = "The index will store temporal values.  This allows it to slightly more efficiently run spatial-temporal queries although if spatial-temporal queries are a common use case, a separate spatial-temporal index is recommended.")
  protected boolean storeTime = false;

  public void storeTime(final boolean storeTime) {
    this.storeTime = storeTime;
  }

  public boolean isStoreTime() {
    return storeTime;
  }
}
