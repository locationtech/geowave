/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.services;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.locationtech.geowave.test.basic.AbstractGeoWaveIT;

public abstract class BaseServiceIT extends AbstractGeoWaveIT {

  private final Map<String, Level> loggerMap = new HashMap<>();

  /*
   * Utility method for dynamically altering the logger level for loggers
   *
   * Note: Slf4j does not expose the setLevel API, so this is using Log4j directly.
   */
  protected synchronized void muteLogging() {
    if (loggerMap.isEmpty()) {
      @SuppressWarnings("unchecked")
      final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
      final Collection<Logger> currentLoggers = ctx.getLoggers();
      final org.apache.logging.log4j.Logger rootLogger = LogManager.getRootLogger();
      currentLoggers.add((Logger) rootLogger);

      currentLoggers.forEach(logger -> {
        loggerMap.put(logger.getName(), logger.getLevel());
        Configurator.setLevel(logger.getName(), Level.OFF);
      });
    }
  }

  protected synchronized void unmuteLogging() {
    loggerMap.entrySet().forEach(entry -> {
      final Map<String, Level> entryLoggerMap = new HashMap<>();
      entryLoggerMap.put(entry.getKey(), entry.getValue());
      Configurator.setLevel(entryLoggerMap);
    });

    Configurator.setRootLevel(Level.WARN);
    loggerMap.clear();
  }
}
