/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.operations;

import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.layout.PatternLayout.Builder;
import org.locationtech.geowave.core.cli.VersionUtils;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "geowave")
@Parameters(commandDescription = "This is the top level section.")
public class GeoWaveTopLevelSection extends DefaultOperation {
  @Parameter(names = "--debug", description = "Verbose output")
  private Boolean verboseFlag;

  @Parameter(names = "--version", description = "Output Geowave build version information")
  private Boolean versionFlag;

  // This contains methods and parameters for determining where the GeoWave
  // cached configuration file is.
  @ParametersDelegate
  private final ConfigOptions options = new ConfigOptions();

  @Override
  public boolean prepare(final OperationParams inputParams) {
    // This will load the properties file parameter into the
    // operation params.
    options.prepare(inputParams);

    super.prepare(inputParams);

    // Up the log level
    if (Boolean.TRUE.equals(verboseFlag)) {
      Configurator.setRootLevel(Level.DEBUG);
      PatternLayout patternLayout =
          PatternLayout.newBuilder().withPattern("%d{dd MMM HH:mm:ss} %p [%c{2}] - %m%n").build();
      PatternLayout.createDefaultLayout();

      ConsoleAppender consoleApp = ConsoleAppender.createDefaultAppenderForLayout(patternLayout);

      ((Logger) LogManager.getRootLogger()).addAppender(consoleApp);
    }

    // Print out the version info if requested.
    if (Boolean.TRUE.equals(versionFlag)) {
      VersionUtils.printVersionInfo(inputParams.getConsole());
      // Do not continue
      return false;
    }

    // Successfully prepared
    return true;
  }
}
