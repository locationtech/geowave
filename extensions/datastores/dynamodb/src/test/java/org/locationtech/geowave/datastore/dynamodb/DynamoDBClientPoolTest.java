/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import org.junit.Test;
import org.locationtech.geowave.datastore.dynamodb.config.DynamoDBOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;

/**
 * The pool exists so that stores pointing at the same DynamoDB share one client. Each client owns a
 * connection pool and an executor and nothing closes them, so handing out a new one per store leaks
 * both.
 */
public class DynamoDBClientPoolTest {

  private static DynamoDBOptions optionsFor(final String endpoint) {
    final DynamoDBOptions options = new DynamoDBOptions();
    options.setEndpoint(endpoint);
    return options;
  }

  @Test
  public void separateOptionsObjectsForTheSameEndpointShareAClient() {
    final AmazonDynamoDBAsync first =
        DynamoDBClientPool.getInstance().getClient(optionsFor("http://localhost:8000"));
    final AmazonDynamoDBAsync second =
        DynamoDBClientPool.getInstance().getClient(optionsFor("http://localhost:8000"));
    assertSame(first, second);
  }

  @Test
  public void differentEndpointsGetDifferentClients() {
    final AmazonDynamoDBAsync first =
        DynamoDBClientPool.getInstance().getClient(optionsFor("http://localhost:8001"));
    final AmazonDynamoDBAsync second =
        DynamoDBClientPool.getInstance().getClient(optionsFor("http://localhost:8002"));
    assertNotSame(first, second);
  }

  @Test
  public void differingConnectionSettingsGetDifferentClients() {
    final DynamoDBOptions first = optionsFor("http://localhost:8003");
    final DynamoDBOptions second = optionsFor("http://localhost:8003");
    second.setMaxConnections(first.getMaxConnections() + 1);
    assertNotSame(
        DynamoDBClientPool.getInstance().getClient(first),
        DynamoDBClientPool.getInstance().getClient(second));
  }
}
