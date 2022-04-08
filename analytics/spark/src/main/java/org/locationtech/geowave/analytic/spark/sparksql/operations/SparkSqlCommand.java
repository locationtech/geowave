/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.sparksql.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.locationtech.geowave.analytic.mapreduce.operations.AnalyticSection;
import org.locationtech.geowave.analytic.spark.sparksql.SqlQueryRunner;
import org.locationtech.geowave.analytic.spark.sparksql.SqlResultsWriter;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.jts.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.beust.jcommander.internal.Console;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@GeowaveOperation(name = "sql", parentOperation = AnalyticSection.class)
@Parameters(commandDescription = "Execute query using SparkSQL")
public class SparkSqlCommand extends ServiceEnabledCommand<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkSqlCommand.class);
  private static final String STORE_ADAPTER_DELIM = "|";
  private static final String CMD_DESCR =
      "<sql query> - e.g. 'select * from %storename["
          + STORE_ADAPTER_DELIM
          + "adaptername"
          + STORE_ADAPTER_DELIM
          + "viewName] where condition...'";

  @Parameter(description = CMD_DESCR)
  private List<String> parameters = new ArrayList<>();

  @ParametersDelegate
  private SparkSqlOptions sparkSqlOptions = new SparkSqlOptions();

  private DataStorePluginOptions outputDataStore = null;
  private final SqlQueryRunner sqlRunner = new SqlQueryRunner();

  // Log some timing
  Stopwatch stopwatch = new Stopwatch();

  @Override
  public void execute(final OperationParams params) throws Exception {
    // Ensure we have all the required arguments
    if (parameters.size() != 1) {
      throw new ParameterException("Requires argument: <sql query>");
    }
    computeResults(params);
  }

  @Override
  public Void computeResults(final OperationParams params) throws Exception {

    // Config file
    final File configFile = getGeoWaveConfigFile(params);

    final String sql = parameters.get(0);

    LOGGER.debug("Input SQL: " + sql);
    final String cleanSql =
        initStores(configFile, sql, sparkSqlOptions.getOutputStoreName(), params.getConsole());

    LOGGER.debug("Running with cleaned SQL: " + cleanSql);
    sqlRunner.setSql(cleanSql);
    sqlRunner.setAppName(sparkSqlOptions.getAppName());
    sqlRunner.setHost(sparkSqlOptions.getHost());
    sqlRunner.setMaster(sparkSqlOptions.getMaster());

    stopwatch.reset();
    stopwatch.start();

    // Execute the query
    final Dataset<Row> results = sqlRunner.run();

    stopwatch.stop();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Spark SQL query took " + stopwatch.getTimeString());
      LOGGER.debug("   and got " + results.count() + " results");
      results.printSchema();
    }

    if (sparkSqlOptions.getShowResults() > 0) {
      results.show(sparkSqlOptions.getShowResults(), false);
    }

    params.getConsole().println("GeoWave SparkSQL query returned " + results.count() + " results");

    if (outputDataStore != null) {
      final SqlResultsWriter sqlResultsWriter = new SqlResultsWriter(results, outputDataStore);

      String typeName = sparkSqlOptions.getOutputTypeName();
      if (typeName == null) {
        typeName = "sqlresults";
      }

      params.getConsole().println("Writing GeoWave SparkSQL query results to datastore...");
      sqlResultsWriter.writeResults(typeName);
      params.getConsole().println("Datastore write complete.");
    }

    if (sparkSqlOptions.getCsvOutputFile() != null) {
      results.repartition(1).write().format("com.databricks.spark.csv").option(
          "header",
          "true").mode(SaveMode.Overwrite).save(sparkSqlOptions.getCsvOutputFile());
    }
    sqlRunner.close();
    return null;
  }

  @SuppressFBWarnings("SF_SWITCH_FALLTHROUGH")
  private String initStores(
      final File configFile,
      final String sql,
      final String outputStoreName,
      final Console console) {
    final Pattern storeDetect = Pattern.compile("(\\\"[^\\\"]*\\\"|'[^']*')|([%][^.,\\s]+)");
    final String escapedDelimRegex = java.util.regex.Pattern.quote(STORE_ADAPTER_DELIM);

    Matcher matchedStore = getFirstPositiveMatcher(storeDetect, sql);
    String replacedSQL = sql;

    while (matchedStore != null) {
      String parseStore = matchedStore.group(2);
      final String originalStoreText = parseStore;

      // Drop the first character off string should be % sign
      parseStore = parseStore.substring(1);
      parseStore = parseStore.trim();

      LOGGER.debug("parsed store: " + parseStore);

      final String[] storeNameParts = parseStore.split(escapedDelimRegex);
      LOGGER.debug("Split Count: " + storeNameParts.length);
      for (final String split : storeNameParts) {
        LOGGER.debug("Store split: " + split);
      }
      String storeName = null;
      String adapterName = null;
      String viewName = null;
      switch (storeNameParts.length) {
        case 3:
          viewName = storeNameParts[2].trim();
        case 2:
          adapterName = storeNameParts[1].trim();
        case 1:
          storeName = storeNameParts[0].trim();
          break;
        default:
          throw new ParameterException(
              "Ambiguous datastore"
                  + STORE_ADAPTER_DELIM
                  + "adapter designation: "
                  + Arrays.toString(storeNameParts));
      }

      // Attempt to load store.
      final DataStorePluginOptions storeOptions =
          CLIUtils.loadStore(storeName, configFile, console);
      viewName = sqlRunner.addInputStore(storeOptions, adapterName, viewName);
      if (viewName != null) {
        replacedSQL = StringUtils.replace(replacedSQL, originalStoreText, viewName, -1);
      }

      matchedStore = getNextPositiveMatcher(matchedStore);
    }

    return replacedSQL;
  }

  private Matcher getFirstPositiveMatcher(final Pattern compiledPattern, final String sql) {
    final Matcher returnMatch = compiledPattern.matcher(sql);
    return getNextPositiveMatcher(returnMatch);
  }

  private Matcher getNextPositiveMatcher(final Matcher lastMatch) {
    while (lastMatch.find()) {
      if (lastMatch.group(2) != null) {
        return lastMatch;
      }
    }
    return null;
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String sql) {
    parameters = new ArrayList<>();
    parameters.add(sql);
  }

  public DataStorePluginOptions getOutputStoreOptions() {
    return outputDataStore;
  }

  public void setOutputStoreOptions(final DataStorePluginOptions outputStoreOptions) {
    outputDataStore = outputStoreOptions;
  }

  public SparkSqlOptions getSparkSqlOptions() {
    return sparkSqlOptions;
  }

  public void setSparkSqlOptions(final SparkSqlOptions sparkSqlOptions) {
    this.sparkSqlOptions = sparkSqlOptions;
  }
}
