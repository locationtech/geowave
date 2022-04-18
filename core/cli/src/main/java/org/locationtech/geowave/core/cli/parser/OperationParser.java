/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.parser;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.cli.api.Operation;
import org.locationtech.geowave.core.cli.prefix.PrefixedJCommander;
import org.locationtech.geowave.core.cli.prefix.PrefixedJCommander.PrefixedJCommanderInitializer;
import org.locationtech.geowave.core.cli.spi.OperationEntry;
import org.locationtech.geowave.core.cli.spi.OperationRegistry;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

public class OperationParser {
  private final OperationRegistry registry;
  private final Set<Object> additionalObjects = new HashSet<>();

  public OperationParser(final OperationRegistry registry) {
    this.registry = registry;
  }

  public OperationParser() {
    this(OperationRegistry.getInstance());
  }

  /**
   * Parse command line arguments into the given operation. The operation will be prepared, and then
   * can be directly executed, or modified before being executed.
   *
   * @param operation the operation
   * @param args the command arguments
   * @return the parsed parameters
   */
  public CommandLineOperationParams parse(final Operation operation, final String[] args) {
    final CommandLineOperationParams params = new CommandLineOperationParams(args);
    final OperationEntry topLevelEntry = registry.getOperation(operation.getClass());
    // Populate the operation map.
    params.getOperationMap().put(topLevelEntry.getOperationNames()[0], operation);
    parseInternal(params, topLevelEntry);
    return params;
  }

  /**
   * Search the arguments for the list of commands/operations to execute based on the top level
   * operation entry given.
   *
   * @param topLevel the top level operation class
   * @param args the command arguments
   * @return the parsed parameters
   */
  public CommandLineOperationParams parse(
      final Class<? extends Operation> topLevel,
      final String[] args) {
    final CommandLineOperationParams params = new CommandLineOperationParams(args);
    final OperationEntry topLevelEntry = registry.getOperation(topLevel);
    parseInternal(params, topLevelEntry);
    return params;
  }

  /**
   * Parse, starting from the given entry.
   *
   * @param params
   */
  private void parseInternal(
      final CommandLineOperationParams params,
      final OperationEntry topLevelEntry) {

    try {
      final PrefixedJCommander pluginCommander = new PrefixedJCommander();
      pluginCommander.setInitializer(new OperationContext(topLevelEntry, params));
      params.setCommander(pluginCommander);
      for (final Object obj : additionalObjects) {
        params.getCommander().addPrefixedObject(obj);
      }

      // Parse without validation so we can prepare.
      params.getCommander().setAcceptUnknownOptions(true);
      params.getCommander().setValidate(false);
      params.getCommander().parse(params.getArgs());

      // Prepare stage:
      for (final Operation operation : params.getOperationMap().values()) {
        // Do not continue
        if (!operation.prepare(params)) {
          params.setSuccessCode(1);
          return;
        }
      }

      // Parse with validation
      final PrefixedJCommander finalCommander = new PrefixedJCommander();
      finalCommander.setInitializer(new OperationContext(topLevelEntry, params));
      params.setCommander(finalCommander);
      for (final Object obj : additionalObjects) {
        params.getCommander().addPrefixedObject(obj);
      }
      params.getCommander().setAcceptUnknownOptions(params.isAllowUnknown());
      params.getCommander().setValidate(params.isValidate());
      params.getCommander().parse(params.getArgs());
    } catch (final ParameterException p) {
      params.setSuccessCode(-1);
      params.setSuccessMessage("Error: " + p.getMessage());
      params.setSuccessException(p);
    }

    return;
  }

  /**
   * Parse the command line arguments into the objects given in the 'additionalObjects' array. I
   * don't really ever forsee this ever being used, but hey, why not.
   *
   * @param args
   */
  public CommandLineOperationParams parse(final String[] args) {

    final CommandLineOperationParams params = new CommandLineOperationParams(args);

    try {
      final PrefixedJCommander pluginCommander = new PrefixedJCommander();
      params.setCommander(pluginCommander);
      for (final Object obj : additionalObjects) {
        params.getCommander().addPrefixedObject(obj);
      }
      params.getCommander().parse(params.getArgs());

    } catch (final ParameterException p) {
      params.setSuccessCode(-1);
      params.setSuccessMessage("Error: " + p.getMessage());
      params.setSuccessException(p);
    }

    return params;
  }

  public Set<Object> getAdditionalObjects() {
    return additionalObjects;
  }

  public void addAdditionalObject(final Object obj) {
    additionalObjects.add(obj);
  }

  public OperationRegistry getRegistry() {
    return registry;
  }

  /**
   * This class is used to lazily init child commands only when they are actually referenced/used by
   * command line options. It will set itself on the commander, and then add its children as
   * commands.
   */
  public class OperationContext implements PrefixedJCommanderInitializer {

    private final OperationEntry operationEntry;
    private final CommandLineOperationParams params;
    private Operation operation;

    public OperationContext(final OperationEntry entry, final CommandLineOperationParams params) {
      operationEntry = entry;
      this.params = params;
    }

    @Override
    public void initialize(final PrefixedJCommander commander) {
      commander.setCaseSensitiveOptions(false);

      final String[] opNames = operationEntry.getOperationNames();
      String opName = opNames[0];
      for (int i = 1; i < opNames.length; i++) {
        for (final String arg : params.getArgs()) {
          if (arg.equals(opNames[i])) {
            opName = arg;
            break;
          }
        }
      }
      // Add myself.
      if (params.getOperationMap().containsKey(opName)) {
        operation = params.getOperationMap().get(opName);
      } else {
        operation = operationEntry.createInstance();
        params.addOperation(opName, operation, operationEntry.isCommand());
      }
      commander.addPrefixedObject(operation);

      // initialize the commander by adding child operations.
      for (final OperationEntry child : operationEntry.getChildren()) {
        final String[] names = child.getOperationNames();
        commander.addCommand(names[0], null, Arrays.copyOfRange(names, 1, names.length));
      }

      // Update each command to add an initializer.
      final Map<String, JCommander> childCommanders = commander.getCommands();
      for (final OperationEntry child : operationEntry.getChildren()) {
        final PrefixedJCommander pCommander =
            (PrefixedJCommander) childCommanders.get(child.getOperationNames()[0]);
        pCommander.setInitializer(new OperationContext(child, params));
      }
    }

    public Operation getOperation() {
      return operation;
    }

    public OperationEntry getOperationEntry() {
      return operationEntry;
    }
  }
}
