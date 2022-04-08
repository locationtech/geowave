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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.locationtech.geowave.datastore.dynamodb.config.DynamoDBOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.beust.jcommander.ParameterException;

public class DynamoDBClientPool {
  private final Logger LOGGER = LoggerFactory.getLogger(DynamoDBClientPool.class);
  private static DynamoDBClientPool singletonInstance;
  private static final int DEFAULT_RETRY_THREADS = 4;
  protected static ExecutorService DYNAMO_RETRY_POOL =
      Executors.newFixedThreadPool(DEFAULT_RETRY_THREADS);

  public static synchronized DynamoDBClientPool getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new DynamoDBClientPool();
    }
    return singletonInstance;
  }

  private final Map<DynamoDBOptions, AmazonDynamoDBAsync> clientCache = new HashMap<>();

  public synchronized AmazonDynamoDBAsync getClient(final DynamoDBOptions options) {
    AmazonDynamoDBAsync client = clientCache.get(options);
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
      clientCache.put(options, client);
    }
    return client;
  }
}
