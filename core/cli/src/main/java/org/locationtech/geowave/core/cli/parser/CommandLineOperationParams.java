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
import java.util.LinkedHashMap;
import java.util.Map;
import org.locationtech.geowave.core.cli.api.Operation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.prefix.PrefixedJCommander;
import com.beust.jcommander.internal.Console;

public class CommandLineOperationParams implements OperationParams {
  private final Map<String, Object> context = new HashMap<>();
  private final Map<String, Operation> operationMap = new LinkedHashMap<>();
  private final String[] args;
  private PrefixedJCommander commander;
  private boolean validate = true;
  private boolean allowUnknown = false;
  private boolean commandPresent;
  private int successCode = 0;
  private String successMessage;
  private Throwable successException;

  public CommandLineOperationParams(final String[] args) {
    this.args = args;
  }

  public String[] getArgs() {
    return args;
  }

  /** Implement parent interface to retrieve operations */
  @Override
  public Map<String, Operation> getOperationMap() {
    return operationMap;
  }

  @Override
  public Map<String, Object> getContext() {
    return context;
  }

  public PrefixedJCommander getCommander() {
    return commander;
  }

  public Console getConsole() {
    return commander.getConsole();
  }

  public void setValidate(final boolean validate) {
    this.validate = validate;
  }

  public void setAllowUnknown(final boolean allowUnknown) {
    this.allowUnknown = allowUnknown;
  }

  public boolean isValidate() {
    return validate;
  }

  public boolean isAllowUnknown() {
    return allowUnknown;
  }

  public void setCommander(final PrefixedJCommander commander) {
    this.commander = commander;
  }

  public void addOperation(final String name, final Operation operation, final boolean isCommand) {
    commandPresent |= isCommand;
    operationMap.put(name, operation);
  }

  public boolean isCommandPresent() {
    return commandPresent;
  }

  public int getSuccessCode() {
    return successCode;
  }

  public void setSuccessCode(final int successCode) {
    this.successCode = successCode;
  }

  public String getSuccessMessage() {
    return successMessage;
  }

  public void setSuccessMessage(final String successMessage) {
    this.successMessage = successMessage;
  }

  public Throwable getSuccessException() {
    return successException;
  }

  public void setSuccessException(final Throwable successException) {
    this.successException = successException;
  }
}
