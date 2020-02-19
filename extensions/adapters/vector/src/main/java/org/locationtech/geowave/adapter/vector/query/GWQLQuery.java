/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.time.StopWatch;
import org.locationtech.geowave.adapter.vector.cli.VectorSection;
import org.locationtech.geowave.adapter.vector.query.gwql.ResultSet;
import org.locationtech.geowave.adapter.vector.query.gwql.parse.GWQLParser;
import org.locationtech.geowave.adapter.vector.query.gwql.statement.Statement;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.store.StoreLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Iterators;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "query", parentOperation = VectorSection.class)
@Parameters(commandDescription = "Query vector data using a GWQL")
public class GWQLQuery extends DefaultOperation implements Command {
  private static Logger LOGGER = LoggerFactory.getLogger(GWQLQuery.class);

  @Parameter(description = "<query>")
  private List<String> parameters = new ArrayList<>();

  @Parameter(
      names = {"-f", "--format"},
      required = false,
      description = "Output format such as console, csv, shp, geojson, etc.")
  private String outputFormat = ConsoleQueryOutputFormat.FORMAT_NAME;

  @ParametersDelegate
  private QueryOutputFormatSpi output;

  @Parameter(
      names = "--debug",
      required = false,
      description = "Print out additional info for debug purposes")
  private final boolean debug = false;

  @Override
  public boolean prepare(final OperationParams params) {
    super.prepare(params);
    final ServiceLoader<QueryOutputFormatSpi> serviceLoader =
        ServiceLoader.load(QueryOutputFormatSpi.class);
    boolean outputFound = false;
    for (final QueryOutputFormatSpi format : serviceLoader) {
      if ((outputFormat != null) && outputFormat.equalsIgnoreCase(format.name())) {
        output = format;
        outputFound = true;
        break;
      }
    }

    if (!outputFound) {
      throw new ParameterException(
          "Not a valid output format. "
              + "Available options are: "
              + Iterators.toString(Iterators.transform(serviceLoader.iterator(), a -> a.name())));
    }
    return true;

  }

  @Override
  public void execute(final OperationParams params) throws ParseException {
    if (debug) {
      org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.DEBUG);
    }

    // Ensure we have all the required arguments
    if (parameters.size() != 1) {
      throw new ParameterException("Requires arguments: <query>");
    }

    final String query = parameters.get(0);
    final Statement statement = GWQLParser.parseStatement(query);
    DataStorePluginOptions storeOptions = null;
    if (statement.getStoreName() != null) {
      final File configFile = (File) params.getContext().get(ConfigOptions.PROPERTIES_FILE_CONTEXT);
      final StoreLoader storeLoader = new StoreLoader(statement.getStoreName());
      if (!storeLoader.loadFromConfig(configFile)) {
        throw new ParameterException("Cannot find store name: " + storeLoader.getStoreName());
      }
      storeOptions = storeLoader.getDataStorePlugin();
    } else {
      throw new ParameterException("Query requires a store name prefix on the type.");
    }
    final StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    ResultSet results = statement.execute(storeOptions.createDataStore());
    stopWatch.stop();
    output.output(results);
    results.close();

    if (debug) {
      LOGGER.debug("Executed query in " + stopWatch.toString());
    }
  }
}
