/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.remote.options;

import com.beust.jcommander.Parameter;
import java.io.Serializable;

public class VisibilityOptions implements Serializable {
  @Parameter(
      names = {"-v", "--visibility"},
      description = "The visibility of the data ingested (optional; default is 'public')")
  private String visibility;

  public String getVisibility() {
    return visibility;
  }

  public void setVisibility(String visibility) {
    this.visibility = visibility;
  }
}
