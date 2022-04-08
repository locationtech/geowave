/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.api;

import java.util.Map;
import com.beust.jcommander.internal.Console;

/**
 * This arguments are used to allow sections and commands to modify how arguments are parsed during
 * prepare / execution stage.
 */
public interface OperationParams {

  /**
   * @return Operations that were parsed & instantiated for execution.
   */
  Map<String, Operation> getOperationMap();

  /**
   * @return Key value pairs for contextual information during command parsing.
   */
  Map<String, Object> getContext();

  /**
   * Get the console to print commandline messages
   * 
   * @return the console
   */
  Console getConsole();
}
