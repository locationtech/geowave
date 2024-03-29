/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.sentinel2;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "providers", parentOperation = Sentinel2Section.class)
@Parameters(commandDescription = "Show info of supported Sentinel2 imagery providers")
public class Sentinel2ImageryProvidersCommand extends DefaultOperation implements Command {

  public Sentinel2ImageryProvidersCommand() {}

  @Override
  public void execute(final OperationParams params) throws Exception {
    System.out.print("Supported Sentinel2 providers:");
    final StringBuilder sb = new StringBuilder();

    for (final Sentinel2ImageryProvider provider : Sentinel2ImageryProvider.getProviders()) {
      sb.append("\n - ").append(provider.providerName()).append(":").append(
          "\n   - Description: ").append(provider.description()).append("\n   - Collections: ");

      for (final String collection : provider.collections()) {
        sb.append(collection).append(", ");
      }
      sb.setLength(sb.length() - 2);
    }
    System.out.println(sb.toString());
    System.out.println();
  }
}
