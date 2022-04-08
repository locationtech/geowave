/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.python.cli;

import com.beust.jcommander.Parameter;
import py4j.GatewayServer;

/**
 * Options for configuring the Py4J Gateway.
 */
public class PythonRunGatewayOptions {
  @Parameter(
      names = "--port",
      arity = 1,
      description = "The port the GatewayServer is listening to.")
  protected int port = 25333;

  @Parameter(
      names = "--pythonPort",
      arity = 1,
      description = "The port used to connect to a Python gateway. Essentially the port used for Python callbacks.")
  protected int pythonPort = 25334;

  @Parameter(
      names = "--address",
      arity = 1,
      description = "The address the GatewayServer is listening to.")
  protected String address = GatewayServer.DEFAULT_ADDRESS;

  @Parameter(
      names = "--pythonAddress",
      arity = 1,
      description = "The address used to connect to a Python gateway.")
  protected String pythonAddress = GatewayServer.DEFAULT_ADDRESS;

  public void setPort(final int port) {
    this.port = port;
  }

  public int getPort() {
    return this.port;
  }

  public void setPythonPort(final int pythonPort) {
    this.pythonPort = pythonPort;
  }

  public int getPythonPort() {
    return this.pythonPort;
  }

  public void setAddress(final String address) {
    this.address = address;
  }

  public String getAddress() {
    return this.address;
  }

  public void setPythonAddress(final String pythonAddress) {
    this.pythonAddress = pythonAddress;
  }

  public String getPythonAddress() {
    return this.pythonAddress;
  }
}
