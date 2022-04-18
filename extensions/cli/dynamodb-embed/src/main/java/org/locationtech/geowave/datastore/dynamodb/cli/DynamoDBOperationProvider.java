/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb.cli;

import org.locationtech.geowave.core.cli.spi.CLIOperationProviderSpi;

public class DynamoDBOperationProvider implements CLIOperationProviderSpi {
  private static final Class<?>[] OPERATIONS =
      new Class<?>[] {DynamoDBSection.class, RunDynamoDBLocal.class};

  @Override
  public Class<?>[] getOperations() {
    return OPERATIONS;
  }

}
