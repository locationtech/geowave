/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.parser;

import java.util.HashMap;
import java.util.Map;
import org.locationtech.geowave.core.cli.api.Operation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.internal.Console;

public class ManualOperationParams implements OperationParams {

  private final Map<String, Object> context = new HashMap<>();

  @Override
  public Map<String, Operation> getOperationMap() {
    return new HashMap<>();
  }

  @Override
  public Map<String, Object> getContext() {
    return context;
  }

  @Override
  public Console getConsole() {
    return new JCommander().getConsole();
  }
}
