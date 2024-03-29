/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.operations;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.parser.CommandLineOperationParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "explain", parentOperation = GeoWaveTopLevelSection.class)
@Parameters(
    commandDescription = "See what arguments are missing and "
        + "what values will be used for GeoWave commands")
public class ExplainCommand extends DefaultOperation implements Command {

  private static Logger LOGGER = LoggerFactory.getLogger(ExplainCommand.class);

  @Override
  public boolean prepare(final OperationParams inputParams) {
    super.prepare(inputParams);
    final CommandLineOperationParams params = (CommandLineOperationParams) inputParams;
    params.setValidate(false);
    params.setAllowUnknown(true);
    // Prepared successfully.
    return true;
  }

  @Override
  public void execute(final OperationParams inputParams) {

    final CommandLineOperationParams params = (CommandLineOperationParams) inputParams;

    final StringBuilder builder = new StringBuilder();

    // Sort first
    String nextCommand = "geowave";
    JCommander commander = params.getCommander();
    while (commander != null) {
      if ((commander.getParameters() != null) && (commander.getParameters().size() > 0)) {
        builder.append("Command: ");
        builder.append(nextCommand);
        builder.append(" [options]");
        if (commander.getParsedCommand() != null) {
          builder.append(" <subcommand> ...");
        }
        builder.append("\n\n");
        builder.append(explainCommander(commander));
        builder.append("\n");
      } else if (commander.getMainParameter() != null) {
        builder.append("Command: ");
        builder.append(nextCommand);
        if (commander.getParsedCommand() != null) {
          builder.append(" <subcommand> ...");
        }
        builder.append("\n\n");
        builder.append(explainMainParameter(commander));
        builder.append("\n");
      }
      nextCommand = commander.getParsedCommand();
      commander = commander.getCommands().get(nextCommand);
    }

    params.getConsole().println(builder.toString().trim());
  }

  /**
   * This function will explain the currently selected values for a JCommander.
   *
   * @param commander
   */
  public static StringBuilder explainCommander(final JCommander commander) {

    final StringBuilder builder = new StringBuilder();

    builder.append(" ");
    builder.append(String.format("%1$20s", "VALUE"));
    builder.append("  ");
    builder.append("NEEDED  ");
    builder.append(String.format("%1$-40s", "PARAMETER NAMES"));
    builder.append("\n");
    builder.append("----------------------------------------------\n");

    // Sort first
    final SortedMap<String, ParameterDescription> parameterDescs = new TreeMap<>();
    final List<ParameterDescription> parameters = commander.getParameters();
    for (final ParameterDescription pd : parameters) {
      parameterDescs.put(pd.getLongestName(), pd);
    }

    // Then output
    for (final ParameterDescription pd : parameterDescs.values()) {

      Object value = null;
      try {
        // value = tEntry.getParam().get(tEntry.getObject());
        value = pd.getParameterized().get(pd.getObject());
      } catch (final Exception e) {
        LOGGER.warn("Unable to set value", e);
      }

      boolean required = false;
      if (pd.getParameterized().getParameter() != null) {
        required = pd.getParameterized().getParameter().required();
      } else if (pd.isDynamicParameter()) {
        required = pd.getParameter().getDynamicParameter().required();
      }

      final String names = pd.getNames();
      final boolean assigned = pd.isAssigned();

      // Data we have:
      // required, assigned, value, names.
      builder.append("{");
      if (value == null) {
        value = "";
      }
      builder.append(String.format("%1$20s", value));
      builder.append("} ");
      if (required && !assigned) {
        builder.append("MISSING ");
      } else {
        builder.append("        ");
      }
      builder.append(String.format("%1$-40s", StringUtils.join(names, ",")));
      builder.append("\n");
    }

    if (commander.getMainParameter() != null) {
      builder.append("\n");
      builder.append(explainMainParameter(commander));
    }

    return builder;
  }

  /**
   * Output details about the main parameter, if there is one.
   *
   * @return the explanation for the main parameter
   */
  @SuppressWarnings("unchecked")
  public static StringBuilder explainMainParameter(final JCommander commander) {
    final StringBuilder builder = new StringBuilder();

    final ParameterDescription mainParameter = commander.getMainParameterValue();

    // Output the main parameter.
    if (mainParameter != null) {
      if ((mainParameter.getDescription() != null)
          && (mainParameter.getDescription().length() > 0)) {
        builder.append("Expects: ");
        builder.append(mainParameter.getDescription());
        builder.append("\n");
      }

      final boolean assigned = mainParameter.isAssigned();
      builder.append("Specified: ");
      final List<String> mP =
          (List<String>) mainParameter.getParameterized().get(mainParameter.getObject());
      if (!assigned || (mP.size() == 0)) {
        builder.append("<none specified>");
      } else {
        builder.append(String.format("%n%s", StringUtils.join(mP, " ")));
      }
      builder.append("\n");
    }

    return builder;
  }
}
