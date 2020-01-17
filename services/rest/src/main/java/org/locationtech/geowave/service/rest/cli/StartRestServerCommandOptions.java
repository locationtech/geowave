/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.cli;

import com.beust.jcommander.Parameter;

public class StartRestServerCommandOptions {
  @Parameter(names = {"--port", "-p"}, description = "The port the emulator will run on")
  private String port = "8182";

  public String getPort() {
    return port;
  }

  public void setPort(String port) {
    this.port = port;
  }
}
