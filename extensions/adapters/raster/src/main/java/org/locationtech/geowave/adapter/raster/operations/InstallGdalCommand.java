/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.operations;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.locationtech.geowave.adapter.raster.plugin.gdal.InstallGdal;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "installgdal", parentOperation = RasterSection.class)
@Parameters(commandDescription = "Install GDAL by downloading native libraries")
public class InstallGdalCommand extends DefaultOperation implements Command {
  private static final String DEFAULT_DOWNLOAD_DIR = "lib/utilities/gdal";

  @Parameter(names = "--dir", description = "The download directory", required = false)
  private String downloadDirectory = null;

  @Override
  public void execute(final OperationParams params) throws Exception {
    if (downloadDirectory == null) {
      final String homeDirectory =
          System.getProperty("geowave.home", DataStoreUtils.DEFAULT_GEOWAVE_DIRECTORY);
      final Path path = Paths.get(homeDirectory, DEFAULT_DOWNLOAD_DIR);
      downloadDirectory = path.toString();
    }
    InstallGdal.main(new String[] {downloadDirectory});
  }
}
