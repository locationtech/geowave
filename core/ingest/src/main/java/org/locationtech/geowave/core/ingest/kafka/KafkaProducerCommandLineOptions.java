/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.kafka;

import com.beust.jcommander.Parameter;

public class KafkaProducerCommandLineOptions extends KafkaCommandLineOptions {

  @PropertyReference("bootstrap.servers")
  @Parameter(
      names = "--bootstrapServers",
      description = "This is for bootstrapping and the producer will only use it for getting metadata (topics, partitions and replicas). The socket connections for sending the actual data will be established based on the broker information returned in the metadata. The format is host1:port1,host2:port2, and the list can be a subset of brokers or a VIP pointing to a subset of brokers.")
  private String bootstrapServers;

  @PropertyReference("retry.backoff.ms")
  @Parameter(
      names = "--retryBackoffMs",
      description = "The amount of time to wait before attempting to retry a failed produce request to a given topic partition. This avoids repeated sending-and-failing in a tight loop.")
  private String retryBackoffMs;

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public void setBootstrapServers(final String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public String getRetryBackoffMs() {
    return retryBackoffMs;
  }

  public void setRetryBackoffMs(final String retryBackoffMs) {
    this.retryBackoffMs = retryBackoffMs;
  }
}
