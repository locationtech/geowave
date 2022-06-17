/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver;

import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.Response;
import org.apache.spark.status.api.v1.ForbiddenException;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.exceptions.DuplicateEntryException;
import org.locationtech.geowave.core.cli.exceptions.TargetNotFoundException;

public abstract class GeoServerCommand<T> extends ServiceEnabledCommand<T> {

  protected GeoServerRestClient geoserverClient = null;

  @Override
  public boolean prepare(final OperationParams params) {
    if (geoserverClient == null) {
      // Create the rest client
      geoserverClient =
          GeoServerRestClient.getInstance(
              new GeoServerConfig(getGeoWaveConfigFile(params), params.getConsole()),
              params.getConsole());
    }

    // Successfully prepared
    return true;
  }

  public boolean isDuplicate(final Response response, final String errorMessage)
      throws TargetNotFoundException {
    if (errorMessage.toLowerCase().contains("already exists")) {
      return true;
    }
    return false;
  }

  public T handleError(final Response response, final String errorMessage) throws Exception {
    if (isDuplicate(response, errorMessage)) {
      throw new DuplicateEntryException(errorMessage);
    }
    switch (response.getStatus()) {
      case 401:
        throw new NotAuthorizedException(errorMessage);
      case 403:
        throw new ForbiddenException(errorMessage);
      case 404:
        throw new TargetNotFoundException(errorMessage);
      // GeoServer responses for 500 codes are poorly formatted so
      // don't return that response
      case 500:
        throw new Exception("Internal Server Error\n GeoServer Response Code = 500");
      default:
        throw new Exception(errorMessage);
    }
  }
}
