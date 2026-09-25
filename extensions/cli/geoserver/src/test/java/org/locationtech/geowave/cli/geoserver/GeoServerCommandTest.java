/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver;

import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.core.Response;
import org.junit.Test;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.exceptions.DuplicateEntryException;
import org.locationtech.geowave.core.cli.exceptions.TargetNotFoundException;

/**
 * The REST services map these exception types to 401, 403, 404 and 400, so they are part of the
 * contract.
 */
public class GeoServerCommandTest {
  private final GeoServerCommand<Void> command = new GeoServerCommand<Void>() {
    @Override
    public void execute(final OperationParams params) {}

    @Override
    public Void computeResults(final OperationParams params) {
      return null;
    }
  };

  @Test(expected = NotAuthorizedException.class)
  public void unauthorized() throws Exception {
    command.handleError(Response.status(401).build(), "denied");
  }

  @Test(expected = ForbiddenException.class)
  public void forbidden() throws Exception {
    command.handleError(Response.status(403).build(), "denied");
  }

  @Test(expected = TargetNotFoundException.class)
  public void notFound() throws Exception {
    command.handleError(Response.status(404).build(), "missing");
  }

  @Test(expected = DuplicateEntryException.class)
  public void duplicate() throws Exception {
    command.handleError(Response.status(500).build(), "Workspace 'x' already exists");
  }
}
