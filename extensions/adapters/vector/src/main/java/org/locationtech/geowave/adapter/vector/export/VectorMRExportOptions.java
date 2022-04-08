/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.export;

import com.beust.jcommander.Parameter;

public class VectorMRExportOptions extends VectorExportOptions {
  @Parameter(names = "--resourceManagerHostPort")
  private String resourceManagerHostPort;

  @Parameter(names = "--minSplits", description = "The min partitions for the input data")
  private Integer minSplits;

  @Parameter(names = "--maxSplits", description = "The max partitions for the input data")
  private Integer maxSplits;

  public Integer getMinSplits() {
    return minSplits;
  }

  public Integer getMaxSplits() {
    return maxSplits;
  }

  public String getResourceManagerHostPort() {
    return resourceManagerHostPort;
  }

  public void setResourceManagerHostPort(final String resourceManagerHostPort) {
    this.resourceManagerHostPort = resourceManagerHostPort;
  }

  public void setMinSplits(final Integer minSplits) {
    this.minSplits = minSplits;
  }

  public void setMaxSplits(final Integer maxSplits) {
    this.maxSplits = maxSplits;
  }
}
