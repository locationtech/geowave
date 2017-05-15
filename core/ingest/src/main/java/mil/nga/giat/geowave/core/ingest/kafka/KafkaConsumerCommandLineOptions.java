/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.ingest.kafka;

import com.beust.jcommander.Parameter;

public class KafkaConsumerCommandLineOptions extends
		KafkaCommandLineOptions
{
	@PropertyReference("group.id")
	@Parameter(names = "--groupId", description = "A string that uniquely identifies the group of consumer processes to which this consumer belongs. By setting the same group id multiple processes indicate that they are all part of the same consumer group.")
	private String groupId;

	@PropertyReference("zookeeper.connect")
	@Parameter(names = "--zookeeperConnect", description = "Specifies the ZooKeeper connection string in the form hostname:port where host and port are the host and port of a ZooKeeper server. To allow connecting through other ZooKeeper nodes when that ZooKeeper machine is down you can also specify multiple hosts in the form hostname1:port1,hostname2:port2,hostname3:port3.")
	private String zookeeperConnect;

	@PropertyReference("auto.offset.reset")
	@Parameter(names = "--autoOffsetReset", description = "What to do when there is no initial offset in ZooKeeper or if an offset is out of range:\n"
			+ "\t* smallest : automatically reset the offset to the smallest offset\n"
			+ "\t* largest : automatically reset the offset to the largest offset\n"
			+ "\t* anything else: throw exception to the consumer\n")
	private String autoOffsetReset;

	@PropertyReference("fetch.message.max.bytes")
	@Parameter(names = "--fetchMessageMaxBytes", description = "The number of bytes of messages to attempt to fetch for each topic-partition in each fetch request. These bytes will be read into memory for each partition, so this helps control the memory used by the consumer. The fetch request size must be at least as large as the maximum message size the server allows or else it is possible for the producer to send messages larger than the consumer can fetch.")
	private String fetchMessageMaxBytes;

	@PropertyReference("consumer.timeout.ms")
	@Parameter(names = "--consumerTimeoutMs", description = "By default, this value is -1 and a consumer blocks indefinitely if no new message is available for consumption. By setting the value to a positive integer, a timeout exception is thrown to the consumer if no message is available for consumption after the specified timeout value.")
	private String consumerTimeoutMs;

	@Parameter(names = "--reconnectOnTimeout", description = "This flag will flush when the consumer timeout occurs (based on kafka property 'consumer.timeout.ms') and immediately reconnect")
	private boolean reconnectOnTimeout = false;

	@Parameter(names = "--batchSize", description = "The data will automatically flush after this number of entries")
	private int batchSize = 10000;

	public boolean isFlushAndReconnect() {
		return reconnectOnTimeout;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(
			String groupId ) {
		this.groupId = groupId;
	}

	public String getZookeeperConnect() {
		return zookeeperConnect;
	}

	public void setZookeeperConnect(
			String zookeeperConnect ) {
		this.zookeeperConnect = zookeeperConnect;
	}

	public String getAutoOffsetReset() {
		return autoOffsetReset;
	}

	public void setAutoOffsetReset(
			String autoOffsetReset ) {
		this.autoOffsetReset = autoOffsetReset;
	}

	public String getFetchMessageMaxBytes() {
		return fetchMessageMaxBytes;
	}

	public void setFetchMessageMaxBytes(
			String fetchMessageMaxBytes ) {
		this.fetchMessageMaxBytes = fetchMessageMaxBytes;
	}

	public String getConsumerTimeoutMs() {
		return consumerTimeoutMs;
	}

	public void setConsumerTimeoutMs(
			String consumerTimeoutMs ) {
		this.consumerTimeoutMs = consumerTimeoutMs;
	}

	public boolean isReconnectOnTimeout() {
		return reconnectOnTimeout;
	}

	public void setReconnectOnTimeout(
			boolean reconnectOnTimeout ) {
		this.reconnectOnTimeout = reconnectOnTimeout;
	}

	public void setBatchSize(
			int batchSize ) {
		this.batchSize = batchSize;
	}

}
