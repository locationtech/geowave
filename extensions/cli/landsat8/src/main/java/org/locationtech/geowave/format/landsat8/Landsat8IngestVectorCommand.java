/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.landsat8;

import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "ingestvector", parentOperation = Landsat8Section.class)
@Parameters(
    commandDescription = "Ingest routine for searching landsat scenes that match certain criteria and ingesting the scene and band metadata into GeoWave's vector store")
public class Landsat8IngestVectorCommand extends DefaultOperation implements Command {

  @Parameter(description = "<store name> <comma delimited index list>")
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  protected Landsat8BasicCommandLineOptions analyzeOptions = new Landsat8BasicCommandLineOptions();

  public Landsat8IngestVectorCommand() {}

  @Override
  public void execute(final OperationParams params) throws Exception {
    final VectorIngestRunner runner = new VectorIngestRunner(analyzeOptions, parameters);
    runner.runInternal(params);
  }
}
