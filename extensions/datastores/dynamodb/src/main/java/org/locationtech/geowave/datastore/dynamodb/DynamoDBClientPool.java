/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.locationtech.geowave.datastore.dynamodb.config.DynamoDBOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.beust.jcommander.ParameterException;

public class DynamoDBClientPool {
  private final Logger LOGGER = LoggerFactory.getLogger(DynamoDBClientPool.class);
  private static DynamoDBClientPool singletonInstance;

  public static synchronized DynamoDBClientPool getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new DynamoDBClientPool();
    }
    return singletonInstance;
  }

  /**
   * What actually distinguishes one client from another. DynamoDBOptions does not define equals or
   * hashCode, and neither does StoreFactoryOptions, so caching against the options object itself
   * was caching against its identity: every store built its own client, each with its own
   * connection pool and executor, and nothing ever closed them.
   */
  private static final class ClientKey {
    private final String endpoint;
    private final Regions region;
    private final Protocol protocol;
    private final int maxConnections;
    private final boolean cacheResponseMetadata;

    ClientKey(final DynamoDBOptions options) {
      endpoint = options.getEndpoint();
      region = options.getRegion();
      final ClientConfiguration config = options.getClientConfig();
      protocol = config.getProtocol();
      maxConnections = config.getMaxConnections();
      cacheResponseMetadata = config.getCacheResponseMetadata();
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ClientKey)) {
        return false;
      }
      final ClientKey other = (ClientKey) obj;
      return Objects.equals(endpoint, other.endpoint)
          && (region == other.region)
          && (protocol == other.protocol)
          && (maxConnections == other.maxConnections)
          && (cacheResponseMetadata == other.cacheResponseMetadata);
    }

    @Override
    public int hashCode() {
      return Objects.hash(endpoint, region, protocol, maxConnections, cacheResponseMetadata);
    }
  }

  private final Map<ClientKey, AmazonDynamoDBAsync> clientCache = new HashMap<>();

  public synchronized AmazonDynamoDBAsync getClient(final DynamoDBOptions options) {
    final ClientKey key = new ClientKey(options);
    AmazonDynamoDBAsync client = clientCache.get(key);
    if (client == null) {

      if ((options.getRegion() == null)
          && ((options.getEndpoint() == null) || options.getEndpoint().isEmpty())) {
        throw new ParameterException("Compulsory to specify either the region or the endpoint");
      }

      final ClientConfiguration clientConfig = options.getClientConfig();
      final AmazonDynamoDBAsyncClientBuilder builder =
          AmazonDynamoDBAsyncClientBuilder.standard().withClientConfiguration(clientConfig);
      if ((options.getEndpoint() != null) && (options.getEndpoint().length() > 0)) {
        builder.withEndpointConfiguration(
            new EndpointConfiguration(
                options.getEndpoint(),
                options.getRegion() != null ? options.getRegion().getName() : "local"));
      } else {
        builder.withRegion(options.getRegion());
      }
      client = builder.build();
      clientCache.put(key, client);
    }
    return client;
  }
}
