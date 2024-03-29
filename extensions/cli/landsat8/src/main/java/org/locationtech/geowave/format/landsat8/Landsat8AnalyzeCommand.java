/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.landsat8;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "analyze", parentOperation = Landsat8Section.class)
@Parameters(
    commandDescription = "Print out basic aggregate statistics for available Landsat 8 imagery")
public class Landsat8AnalyzeCommand extends DefaultOperation implements Command {
  @ParametersDelegate
  protected Landsat8BasicCommandLineOptions landsatOptions = new Landsat8BasicCommandLineOptions();

  public Landsat8AnalyzeCommand() {}

  @Override
  public void execute(final OperationParams params) throws Exception {
    final AnalyzeRunner runner = new AnalyzeRunner(landsatOptions);
    runner.runInternal(params);
  }
}
