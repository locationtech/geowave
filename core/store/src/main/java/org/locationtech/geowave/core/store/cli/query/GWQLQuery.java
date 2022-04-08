/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.GeoWaveTopLevelSection;
import org.locationtech.geowave.core.index.SPIServiceRegistry;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.query.gwql.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.collect.Iterators;

@GeowaveOperation(name = "query", parentOperation = GeoWaveTopLevelSection.class)
@Parameters(commandDescription = "Query vector data using a GWQL")
public class GWQLQuery extends DefaultOperation implements Command {
  private static Logger LOGGER = LoggerFactory.getLogger(GWQLQuery.class);

  @Parameter(description = "<store name> <query>")
  private List<String> parameters = new ArrayList<>();

  @Parameter(
      names = {"-f", "--format"},
      required = false,
      description = "Output format such as console, csv, shp, geojson, etc.")
  private String outputFormat = ConsoleQueryOutputFormat.FORMAT_NAME;

  @Parameter(
      names = {"-a", "--authorization"},
      required = false,
      description = "Authorization to use.  Can be specified multiple times.")
  private List<String> authorizations = new ArrayList<>();

  @ParametersDelegate
  private QueryOutputFormatSpi output;

  @Parameter(
      names = "--debug",
      required = false,
      description = "Print out additional info for debug purposes")
  private boolean debug = false;

  public void setOutputFormat(final String outputFormat) {
    this.outputFormat = outputFormat;
  }

  public void setDebug(final boolean debug) {
    this.debug = debug;
  }

  public void setParameters(final List<String> parameters) {
    this.parameters = parameters;
  }

  public void setAuthorizations(final List<String> authorizations) {
    this.authorizations = authorizations;
  }

  @Override
  public boolean prepare(final OperationParams params) {
    super.prepare(params);
    final Iterator<QueryOutputFormatSpi> spiIter =
        new SPIServiceRegistry(GWQLQuery.class).load(QueryOutputFormatSpi.class);
    boolean outputFound = false;
    while (spiIter.hasNext()) {
      final QueryOutputFormatSpi format = spiIter.next();
      if ((outputFormat != null) && outputFormat.equalsIgnoreCase(format.name())) {
        output = format;
        if (output instanceof ConsoleQueryOutputFormat) {
          ((ConsoleQueryOutputFormat) output).setConsole(params.getConsole());
        }
        outputFound = true;
        break;
      }
    }

    if (!outputFound) {
      throw new ParameterException(
          "Not a valid output format. "
              + "Available options are: "
              + Iterators.toString(Iterators.transform(spiIter, a -> a.name())));
    }
    return true;

  }

  @Override
  public void execute(final OperationParams params) {
    if (debug) {
      Configurator.setLevel(LogManager.getRootLogger().getName(), Level.DEBUG);
    }

    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException("Requires arguments: <store name> <query>");
    }


    final String storeName = parameters.get(0);

    // Attempt to load store.
    final DataStorePluginOptions inputStoreOptions =
        CLIUtils.loadStore(storeName, getGeoWaveConfigFile(params), params.getConsole());

    final String query = parameters.get(1);
    final StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    final ResultSet results =
        inputStoreOptions.createDataStore().query(
            query,
            authorizations.toArray(new String[authorizations.size()]));
    stopWatch.stop();
    output.output(results);
    results.close();

    if (debug) {
      LOGGER.debug("Executed query in " + stopWatch.toString());
    }
  }
}
