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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.locationtech.geowave.core.cli.api.Operation;

/**
 * This implementation uses the SPI to load all Operations across the program, including those
 * exported by plugins. It parses the entries and places them into a cache.
 */
public class OperationRegistry {

  private Map<Class<?>, OperationEntry> operationMapByClass = null;

  /** Singleton pattern allows us to create a version that can be used by the whole application. */
  private static class OperationRegistryHolder {
    public static final OperationRegistry instance = new OperationRegistry();
  }

  /** But also allow the user to create their own if they want it to be sanitized. */
  public OperationRegistry() {
    init();
  }

  public static OperationRegistry getInstance() {
    return OperationRegistryHolder.instance;
  }

  public OperationRegistry(final List<OperationEntry> entries) {
    operationMapByClass = new HashMap<>();
    for (final OperationEntry entry : entries) {
      operationMapByClass.put(entry.getOperationClass(), entry);
    }
  }

  private synchronized void init() {
    if (operationMapByClass == null) {
      operationMapByClass = new HashMap<>();
      // Load SPI elements
      final Iterator<CLIOperationProviderSpi> operationProviders =
          ServiceLoader.load(CLIOperationProviderSpi.class).iterator();
      while (operationProviders.hasNext()) {
        final CLIOperationProviderSpi operationProvider = operationProviders.next();
        for (final Class<?> clz : operationProvider.getOperations()) {
          if (Operation.class.isAssignableFrom(clz)) {
            final OperationEntry entry = new OperationEntry(clz);
            operationMapByClass.put(clz, entry);
          } else {
            throw new RuntimeException(
                "CLI operations must be assignable from Operation.class: "
                    + clz.getCanonicalName());
          }
        }
      }

      // Build a hierarchy.
      for (final OperationEntry entry : operationMapByClass.values()) {
        if (!entry.isTopLevel()) {
          final OperationEntry parentEntry =
              operationMapByClass.get(entry.getParentOperationClass());
          if (parentEntry == null) {
            throw new RuntimeException(
                "Cannot find parent entry for " + entry.getOperationClass().getName());
          }
          if (parentEntry.isCommand()) {
            throw new RuntimeException(
                "Cannot have a command be a parent: " + entry.getClass().getCanonicalName());
          }
          parentEntry.addChild(entry);
        }
      }
    }
  }

  /**
   * @return a collection of all entries to allow for iteration and exploration by the caller
   */
  public Collection<OperationEntry> getAllOperations() {
    return Collections.unmodifiableCollection(operationMapByClass.values());
  }

  /**
   * Get the exported service entry by class name.
   *
   * @param operationClass
   * @return the operation entry, if it exists
   */
  public OperationEntry getOperation(final Class<?> operationClass) {
    return operationMapByClass.get(operationClass);
  }
}
