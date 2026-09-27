/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import java.net.URI;
import java.util.Optional;
import org.junit.Test;
import org.locationtech.geowave.datastore.dynamodb.config.DynamoDBOptions;
import org.locationtech.geowave.datastore.dynamodb.config.DynamoDBOptions.Protocol;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * The pool exists so that stores pointing at the same DynamoDB share one client. Each client owns a
 * connection pool, so handing out a new one per store leaks it.
 */
public class DynamoDBClientPoolTest {

  private static DynamoDBOptions optionsFor(final String endpoint) {
    final DynamoDBOptions options = new DynamoDBOptions();
    options.setEndpoint(endpoint);
    return options;
  }

  private static DynamoDBOptions optionsFor(final Region region, final Protocol protocol) {
    final DynamoDBOptions options = new DynamoDBOptions();
    options.setRegion(region);
    options.setProtocol(protocol);
    return options;
  }

  private static Optional<URI> endpointOverride(final DynamoDbClient client) {
    return client.serviceClientConfiguration().endpointOverride();
  }

  @Test
  public void separateOptionsObjectsForTheSameEndpointShareAClient() {
    final DynamoDbClient first =
        DynamoDBClientPool.getInstance().getClient(optionsFor("http://localhost:8000"));
    final DynamoDbClient second =
        DynamoDBClientPool.getInstance().getClient(optionsFor("http://localhost:8000"));
    assertSame(first, second);
  }

  @Test
  public void separateOptionsObjectsForTheSameRegionShareAClient() {
    final DynamoDbClient first =
        DynamoDBClientPool.getInstance().getClient(
            optionsFor(new DynamoDBOptions.RegionConverter().convert("US_WEST_2"), Protocol.HTTPS));
    final DynamoDbClient second =
        DynamoDBClientPool.getInstance().getClient(optionsFor(Region.US_WEST_2, Protocol.HTTPS));
    assertSame(first, second);
  }

  @Test
  public void differentEndpointsGetDifferentClients() {
    final DynamoDbClient first =
        DynamoDBClientPool.getInstance().getClient(optionsFor("http://localhost:8001"));
    final DynamoDbClient second =
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

  @Test
  public void closingThePoolStartsItAfresh() {
    final DynamoDBOptions options = optionsFor("http://localhost:8006");
    final DynamoDbClient closed = DynamoDBClientPool.getInstance().getClient(options);
    DynamoDBClientPool.getInstance().closeAll();
    assertNotSame(closed, DynamoDBClientPool.getInstance().getClient(options));
  }

  @Test
  public void anEndpointWithoutASchemeTakesTheProtocol() {
    final DynamoDBOptions options = optionsFor("localhost:8004");
    options.setProtocol(Protocol.HTTP);
    assertEquals(
        Optional.of(URI.create("http://localhost:8004")),
        endpointOverride(DynamoDBClientPool.getInstance().getClient(options)));
  }

  @Test
  public void anEndpointWithoutARegionStillHasOneToSignWith() {
    assertEquals(
        "local",
        DynamoDBClientPool.getInstance().getClient(
            optionsFor("http://localhost:8005")).serviceClientConfiguration().region().id());
  }

  @Test
  public void aRegionUsesItsOwnEndpointOverHttpsByDefault() {
    assertFalse(
        endpointOverride(
            DynamoDBClientPool.getInstance().getClient(
                optionsFor(Region.EU_WEST_1, Protocol.HTTPS))).isPresent());
  }

  @Test
  public void aRegionOverHttpKeepsItsOwnHost() {
    assertEquals(
        Optional.of(URI.create("http://dynamodb.eu-west-1.amazonaws.com")),
        endpointOverride(
            DynamoDBClientPool.getInstance().getClient(
                optionsFor(Region.EU_WEST_1, Protocol.HTTP))));
  }
}
