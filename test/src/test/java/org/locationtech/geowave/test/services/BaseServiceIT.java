/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.services;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
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
      final List<Logger> currentLoggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
      currentLoggers.add(LogManager.getRootLogger());

      currentLoggers.forEach(logger -> {
        loggerMap.put(logger.getName(), logger.getEffectiveLevel());
        logger.setLevel(Level.OFF);
      });
    }
  }

  protected synchronized void unmuteLogging() {
    loggerMap.entrySet().forEach(entry -> {
      LogManager.getLogger(entry.getKey()).setLevel(entry.getValue());
    });
    loggerMap.clear();
  }
}
