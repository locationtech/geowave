/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.stats;

import java.io.IOException;
import java.util.List;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import com.beust.jcommander.ParametersDelegate;
import com.beust.jcommander.internal.Console;

/** Common methods for dumping, manipulating and calculating stats. */
public abstract class AbstractStatsCommand<T> extends ServiceEnabledCommand<T> {

  /** Return "200 OK" for all stats commands. */
  @Override
  public Boolean successStatusIs200() {
    return true;
  }

  @ParametersDelegate
  private StatsCommandLineOptions statsOptions = new StatsCommandLineOptions();

  public void run(final OperationParams params, final List<String> parameters) {
    DataStorePluginOptions inputStoreOptions = null;
    if (parameters.size() > 0) {
      final String storeName = parameters.get(0);

      // Attempt to load store.
      inputStoreOptions =
          CLIUtils.loadStore(storeName, getGeoWaveConfigFile(params), params.getConsole());
    }
    try {
      performStatsCommand(inputStoreOptions, statsOptions, params.getConsole());
    } catch (final IOException e) {
      throw new RuntimeException("Unable to parse stats tool arguments", e);
    }
  }

  public void setStatsOptions(final StatsCommandLineOptions statsOptions) {
    this.statsOptions = statsOptions;
  }

  /** Abstracted command method to be called when command selected */
  protected abstract boolean performStatsCommand(
      final DataStorePluginOptions options,
      final StatsCommandLineOptions statsOptions,
      final Console console) throws IOException;

  /**
   * Helper method to extract a list of authorizations from a string passed in from the command line
   *
   * @param auths - String to be parsed
   */
  protected static String[] getAuthorizations(final String auths) {
    if ((auths == null) || (auths.length() == 0)) {
      return new String[0];
    }
    final String[] authsArray = auths.split(",");
    for (int i = 0; i < authsArray.length; i++) {
      authsArray[i] = authsArray[i].trim();
    }
    return authsArray;
  }
}
