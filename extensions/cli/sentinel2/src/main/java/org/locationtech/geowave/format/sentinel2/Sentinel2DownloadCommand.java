/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.sentinel2;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "download", parentOperation = Sentinel2Section.class)
@Parameters(commandDescription = "Download Sentinel2 imagery to a local directory.")
public class Sentinel2DownloadCommand extends DefaultOperation implements Command {
  @ParametersDelegate
  protected Sentinel2BasicCommandLineOptions analyzeOptions =
      new Sentinel2BasicCommandLineOptions();

  @ParametersDelegate
  protected Sentinel2DownloadCommandLineOptions downloadOptions =
      new Sentinel2DownloadCommandLineOptions();

  public Sentinel2DownloadCommand() {}

  @Override
  public void execute(final OperationParams params) throws Exception {
    final DownloadRunner runner = new DownloadRunner(analyzeOptions, downloadOptions);
    runner.runInternal(params);
  }
}
