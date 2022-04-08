/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.operations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.Operation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.parser.CommandLineOperationParams;
import org.locationtech.geowave.core.cli.prefix.JCommanderPrefixTranslator;
import org.locationtech.geowave.core.cli.prefix.JCommanderTranslationMap;
import org.locationtech.geowave.core.cli.spi.OperationEntry;
import org.locationtech.geowave.core.cli.spi.OperationRegistry;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "help", parentOperation = GeoWaveTopLevelSection.class)
@Parameters(commandDescription = "Get descriptions of arguments for any GeoWave command")
public class HelpCommand extends DefaultOperation implements Command {

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

    final List<String> nameArray = new ArrayList<>();
    final OperationRegistry registry = OperationRegistry.getInstance();

    StringBuilder builder = new StringBuilder();

    Operation lastOperation = null;
    for (final Map.Entry<String, Operation> entry : params.getOperationMap().entrySet()) {
      if (entry.getValue() == this) {
        continue;
      }
      nameArray.add(entry.getKey());
      lastOperation = entry.getValue();
    }

    if (lastOperation == null) {
      lastOperation = registry.getOperation(GeoWaveTopLevelSection.class).createInstance();
    }
    if (lastOperation != null) {
      final String usage = lastOperation.usage();
      if (usage != null) {
        System.out.println(usage);
      } else {
        // This is done because if we don't, then JCommander will
        // consider the given parameters as the Default parameters.
        // It's also done so that we can parse prefix annotations
        // and special delegate processing.
        final JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();

        translator.addObject(lastOperation);
        final JCommanderTranslationMap map = translator.translate();
        map.createFacadeObjects();

        // Copy default parameters over for help display.
        map.transformToFacade();

        // Execute a prepare

        // Add processed objects
        final JCommander jc = new JCommander();
        for (final Object obj : map.getObjects()) {
          jc.addObject(obj);
        }

        final String programName = StringUtils.join(nameArray, " ");
        jc.setProgramName(programName);
        jc.getUsageFormatter().usage(builder);

        // Trim excess newlines.
        final String operations = builder.toString().trim();
        builder = new StringBuilder();
        builder.append(operations);
        builder.append("\n\n");

        // Add sub-commands
        final OperationEntry lastEntry = registry.getOperation(lastOperation.getClass());
        // Cast to list so we can sort it based on operation name.
        final List<OperationEntry> children = new ArrayList<>(lastEntry.getChildren());
        Collections.sort(children, getOperationComparator());
        if (children.size() > 0) {
          builder.append("  Commands:\n");
          for (final OperationEntry childEntry : children) {

            // Get description annotation
            final Parameters p = childEntry.getOperationClass().getAnnotation(Parameters.class);

            // If not hidden, then output it.
            if ((p == null) || !p.hidden()) {
              builder.append(
                  String.format(
                      "    %s%n",
                      StringUtils.join(childEntry.getOperationNames(), ", ")));
              if (p != null) {
                final String description = p.commandDescription();
                builder.append(String.format("      %s%n", description));
              } else {
                builder.append("      <no description>\n");
              }
              builder.append("\n");
            }
          }
        }

        // Trim excess newlines.
        final String output = builder.toString().trim();

        System.out.println(output);
      }
    }
  }

  /**
   * This will sort operations based on their name. Just looks prettier on output.
   *
   * @return
   */
  private Comparator<OperationEntry> getOperationComparator() {
    return new Comparator<OperationEntry>() {
      @Override
      public int compare(final OperationEntry o1, final OperationEntry o2) {
        return o1.getOperationNames()[0].compareTo(o2.getOperationNames()[0]);
      }
    };
  }
}
