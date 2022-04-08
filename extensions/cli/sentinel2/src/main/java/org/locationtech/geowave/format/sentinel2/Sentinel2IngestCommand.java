/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.sentinel2;

import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import it.geosolutions.jaiext.JAIExt;

@GeowaveOperation(name = "ingest", parentOperation = Sentinel2Section.class)
@Parameters(
    commandDescription = "Ingest routine for locally downloading Sentinel2 imagery and ingesting it into GeoWave's raster store and in parallel ingesting the scene metadata into GeoWave's vector store.  These two stores can actually be the same or they can be different.")
public class Sentinel2IngestCommand extends DefaultOperation implements Command {
  @Parameter(description = "<store name> <comma delimited index list>")
  private List<String> parameters = new ArrayList<String>();

  @ParametersDelegate
  protected Sentinel2BasicCommandLineOptions analyzeOptions =
      new Sentinel2BasicCommandLineOptions();

  @ParametersDelegate
  protected Sentinel2DownloadCommandLineOptions downloadOptions =
      new Sentinel2DownloadCommandLineOptions();

  @ParametersDelegate
  protected Sentinel2RasterIngestCommandLineOptions ingestOptions =
      new Sentinel2RasterIngestCommandLineOptions();

  @ParametersDelegate
  protected VectorOverrideCommandLineOptions vectorOverrideOptions =
      new VectorOverrideCommandLineOptions();

  public Sentinel2IngestCommand() {}

  @Override
  public void execute(final OperationParams params) throws Exception {
    JAIExt.initJAIEXT();

    final IngestRunner runner =
        new IngestRunner(
            analyzeOptions,
            downloadOptions,
            ingestOptions,
            vectorOverrideOptions,
            parameters);
    runner.runInternal(params);
  }
}
