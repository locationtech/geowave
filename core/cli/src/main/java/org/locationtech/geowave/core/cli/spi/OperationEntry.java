/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.spi;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An operation entry represents an Operation Parsed from SPI, which is then subsequently added to
 * an OperationExecutor for execution.
 */
public final class OperationEntry {
  private static Logger LOGGER = LoggerFactory.getLogger(OperationEntry.class);

  private final String[] operationNames;
  private final Class<?> operationClass;
  private final Class<?> parentOperationClass;
  private final Map<String, OperationEntry> childrenMap;
  private final List<OperationEntry> children;
  private final boolean command;
  private final boolean topLevel;

  public OperationEntry(final Class<?> operationClass) {
    this.operationClass = operationClass;
    final GeowaveOperation operation = this.operationClass.getAnnotation(GeowaveOperation.class);
    if (operation == null) {
      throw new RuntimeException(
          "Expected Operation class to use GeowaveOperation annotation: "
              + this.operationClass.getCanonicalName());
    }
    operationNames = operation.name();
    parentOperationClass = operation.parentOperation();
    command = Command.class.isAssignableFrom(operationClass);
    topLevel = (parentOperationClass == null) || (parentOperationClass == Object.class);
    childrenMap = new HashMap<>();
    children = new LinkedList<>();
  }

  public Class<?> getParentOperationClass() {
    return parentOperationClass;
  }

  public String[] getOperationNames() {
    return operationNames;
  }

  public Class<?> getOperationClass() {
    return operationClass;
  }

  public Collection<OperationEntry> getChildren() {
    return Collections.unmodifiableCollection(children);
  }

  public void addChild(final OperationEntry child) {
    for (final String name : child.getOperationNames()) {
      if (childrenMap.containsKey(name.toLowerCase(Locale.ENGLISH))) {
        throw new RuntimeException(
            "Duplicate operation name: " + name + " for " + getOperationClass().getName());
      }
      childrenMap.put(name.toLowerCase(Locale.ENGLISH), child);
    }
    children.add(child);
  }

  public OperationEntry getChild(final String name) {
    return childrenMap.get(name);
  }

  public boolean isCommand() {
    return command;
  }

  public boolean isTopLevel() {
    return topLevel;
  }

  public Operation createInstance() {
    try {
      return (Operation) operationClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      LOGGER.error("Unable to create new instance", e);
      return null;
    }
  }
}
