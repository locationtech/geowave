/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.operations.config;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "list", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "List GeoWave configuration properties")
public class ListCommand extends ServiceEnabledCommand<SortedMap<String, Object>> {

  @Parameter(names = {"-f", "--filter"})
  private String filter;

  @Override
  public void execute(final OperationParams params) {
    final Pair<String, SortedMap<String, Object>> list = getProperties(params);
    final String name = list.getKey();

    params.getConsole().println("PROPERTIES (" + name + ")");

    final SortedMap<String, Object> properties = list.getValue();

    for (final Entry<String, Object> e : properties.entrySet()) {
      params.getConsole().println(e.getKey() + ": " + e.getValue());
    }
  }

  @Override
  public SortedMap<String, Object> computeResults(final OperationParams params) {

    return getProperties(params).getValue();
  }

  private Pair<String, SortedMap<String, Object>> getProperties(final OperationParams params) {

    final File f = getGeoWaveConfigFile(params);

    // Reload options with filter if specified.
    Properties p = null;
    if (filter != null) {
      p = ConfigOptions.loadProperties(f, filter);
    } else {
      p = ConfigOptions.loadProperties(f);
    }
    return new ImmutablePair<>(f.getName(), new GeoWaveConfig(p));
  }

  protected static class GeoWaveConfig extends TreeMap<String, Object> {

    private static final long serialVersionUID = 1L;

    public GeoWaveConfig() {
      super();
    }

    public GeoWaveConfig(final Map m) {
      super(m);
    }
  }
}
