/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import com.beust.jcommander.Parameter;

public class KafkaConsumerCommandLineOptions extends KafkaCommandLineOptions {
  @PropertyReference("group.id")
  @Parameter(
      names = "--groupId",
      description = "A string that uniquely identifies the group of consumer processes to which this consumer belongs. By setting the same group id multiple processes indicate that they are all part of the same consumer group.")
  private String groupId;

  @PropertyReference(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
  @Parameter(
      names = "--bootstrapServers",
      description = "This is for bootstrapping and the consumer will only use it for getting metadata (topics, partitions and replicas). The socket connections for sending the actual data will be established based on the broker information returned in the metadata. The format is host1:port1,host2:port2, and the list can be a subset of brokers or a VIP pointing to a subset of brokers.")
  private String bootstrapServers;

  @PropertyReference("auto.offset.reset")
  @Parameter(
      names = "--autoOffsetReset",
      description = "What to do when there is no initial offset in ZooKeeper or if an offset is out of range:\n"
          + "\t* earliest: automatically reset the offset to the earliest offset\n"
          + "\t* latest: automatically reset the offset to the latest offset\n"
          + "\t* none: don't reset the offset\n"
          + "\t* anything else: throw exception to the consumer\n")
  private String autoOffsetReset;

  @PropertyReference("max.partition.fetch.bytes")
  @Parameter(
      names = "--maxPartitionFetchBytes",
      description = "The number of bytes of messages to attempt to fetch for each topic-partition in each fetch request. These bytes will be read into memory for each partition, so this helps control the memory used by the consumer. The fetch request size must be at least as large as the maximum message size the server allows or else it is possible for the producer to send messages larger than the consumer can fetch.")
  private String maxPartitionFetchBytes;

  @Parameter(
      names = "--consumerTimeoutMs",
      description = "By default, this value is -1 and a consumer blocks indefinitely if no new message is available for consumption. By setting the value to a positive integer, a timeout exception is thrown to the consumer if no message is available for consumption after the specified timeout value.")
  private String consumerTimeoutMs;

  @Parameter(
      names = "--reconnectOnTimeout",
      description = "This flag will flush when the consumer timeout occurs (based on kafka property 'consumer.timeout.ms') and immediately reconnect")
  private boolean reconnectOnTimeout = false;

  @Parameter(
      names = "--batchSize",
      description = "The data will automatically flush after this number of entries")
  private int batchSize = 10000;

  public boolean isFlushAndReconnect() {
    return reconnectOnTimeout;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public void setBootstrapServers(final String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(final String groupId) {
    this.groupId = groupId;
  }

  public String getAutoOffsetReset() {
    return autoOffsetReset;
  }

  public void setAutoOffsetReset(final String autoOffsetReset) {
    this.autoOffsetReset = autoOffsetReset;
  }

  public String getMaxPartitionFetchBytes() {
    return maxPartitionFetchBytes;
  }

  public void setMaxPartitionFetchBytes(final String maxPartitionFetchBytes) {
    this.maxPartitionFetchBytes = maxPartitionFetchBytes;
  }

  public String getConsumerTimeoutMs() {
    return consumerTimeoutMs;
  }

  public void setConsumerTimeoutMs(final String consumerTimeoutMs) {
    this.consumerTimeoutMs = consumerTimeoutMs;
  }

  public boolean isReconnectOnTimeout() {
    return reconnectOnTimeout;
  }

  public void setReconnectOnTimeout(final boolean reconnectOnTimeout) {
    this.reconnectOnTimeout = reconnectOnTimeout;
  }

  public void setBatchSize(final int batchSize) {
    this.batchSize = batchSize;
  }
}
