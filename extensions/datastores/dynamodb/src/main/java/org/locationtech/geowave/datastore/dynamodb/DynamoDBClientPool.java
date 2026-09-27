/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.locationtech.geowave.datastore.dynamodb.config.DynamoDBOptions;
import org.locationtech.geowave.datastore.dynamodb.config.DynamoDBOptions.Protocol;
import com.beust.jcommander.ParameterException;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

public class DynamoDBClientPool {
  /** The signing region for an endpoint given without one, which DynamoDB Local accepts. */
  private static final Region ENDPOINT_ONLY_REGION = Region.of("local");

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
   * connection pool, and nothing ever closed them.
   */
  private static final class ClientKey {
    private final String endpoint;
    // Region does not define equals
    private final String regionId;
    private final Protocol protocol;
    private final int maxConnections;

    ClientKey(final DynamoDBOptions options) {
      endpoint = options.getEndpoint();
      regionId = options.getRegion() == null ? null : options.getRegion().id();
      protocol = options.getProtocol();
      maxConnections = options.getMaxConnections();
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
          && Objects.equals(regionId, other.regionId)
          && (protocol == other.protocol)
          && (maxConnections == other.maxConnections);
    }

    @Override
    public int hashCode() {
      return Objects.hash(endpoint, regionId, protocol, maxConnections);
    }
  }

  private final Map<ClientKey, DynamoDbClient> clientCache = new HashMap<>();

  public synchronized DynamoDbClient getClient(final DynamoDBOptions options) {
    final ClientKey key = new ClientKey(options);
    DynamoDbClient client = clientCache.get(key);
    if (client == null) {
      final boolean hasEndpoint =
          (options.getEndpoint() != null) && !options.getEndpoint().isEmpty();
      if ((options.getRegion() == null) && !hasEndpoint) {
        throw new ParameterException("Compulsory to specify either the region or the endpoint");
      }

      final DynamoDbClientBuilder builder =
          DynamoDbClient.builder().httpClientBuilder(
              ApacheHttpClient.builder().maxConnections(options.getMaxConnections()));
      if (hasEndpoint) {
        builder.endpointOverride(withScheme(options.getEndpoint(), options.getProtocol())).region(
            options.getRegion() != null ? options.getRegion() : ENDPOINT_ONLY_REGION);
      } else {
        builder.region(options.getRegion());
        if (options.getProtocol() == Protocol.HTTP) {
          // SDK v2 resolves every regional endpoint to HTTPS, where v1 honoured the protocol
          builder.endpointOverride(
              withScheme(
                  DynamoDbClient.serviceMetadata().endpointFor(options.getRegion()).toString(),
                  Protocol.HTTP));
        }
      }
      client = builder.build();
      clientCache.put(key, client);
    }
    return client;
  }

  /**
   * Closes every client handed out so far. Stores still holding one can no longer use it, so this
   * is for shutting down, such as at the end of a test suite.
   */
  public synchronized void closeAll() {
    clientCache.values().forEach(DynamoDbClient::close);
    clientCache.clear();
  }

  /** SDK v1 applied the configured protocol to an endpoint given without a scheme; v2 needs one. */
  private static URI withScheme(final String endpoint, final Protocol protocol) {
    return URI.create(endpoint.contains("://") ? endpoint : protocol.scheme() + "://" + endpoint);
  }
}
