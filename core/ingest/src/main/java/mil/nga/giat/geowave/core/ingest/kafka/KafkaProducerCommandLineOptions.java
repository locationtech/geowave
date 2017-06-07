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

public class KafkaProducerCommandLineOptions extends
		KafkaCommandLineOptions
{

	@PropertyReference("metadata.broker.list")
	@Parameter(names = "--metadataBrokerList", description = "This is for bootstrapping and the producer will only use it for getting metadata (topics, partitions and replicas). The socket connections for sending the actual data will be established based on the broker information returned in the metadata. The format is host1:port1,host2:port2, and the list can be a subset of brokers or a VIP pointing to a subset of brokers.")
	private String metadataBrokerList;

	@PropertyReference("request.required.acks")
	@Parameter(names = "--requestRequiredAcks", description = "This value controls when a produce request is considered completed. Specifically, how many other brokers must have committed the data to their log and acknowledged this to the leader?")
	private String requestRequiredAcks;

	@PropertyReference("producer.type")
	@Parameter(names = "--producerType", description = "This parameter specifies whether the messages are sent asynchronously in a background thread. Valid values are (1) async for asynchronous send and (2) sync for synchronous send. By setting the producer to async we allow batching together of requests (which is great for throughput) but open the possibility of a failure of the client machine dropping unsent data.")
	private String producerType;

	@PropertyReference("serializer.class")
	@Parameter(names = "--serializerClass", description = "The serializer class for messages. The default encoder takes a byte[] and returns the same byte[].")
	private String serializerClass;

	@PropertyReference("retry.backoff.ms")
	@Parameter(names = "--retryBackoffMs", description = "The amount of time to wait before attempting to retry a failed produce request to a given topic partition. This avoids repeated sending-and-failing in a tight loop.")
	private String retryBackoffMs;

	public String getMetadataBrokerList() {
		return metadataBrokerList;
	}

	public void setMetadataBrokerList(
			String metadataBrokerList ) {
		this.metadataBrokerList = metadataBrokerList;
	}

	public String getRequestRequiredAcks() {
		return requestRequiredAcks;
	}

	public void setRequestRequiredAcks(
			String requestRequiredAcks ) {
		this.requestRequiredAcks = requestRequiredAcks;
	}

	public String getProducerType() {
		return producerType;
	}

	public void setProducerType(
			String producerType ) {
		this.producerType = producerType;
	}

	public String getSerializerClass() {
		return serializerClass;
	}

	public void setSerializerClass(
			String serializerClass ) {
		this.serializerClass = serializerClass;
	}

	public String getRetryBackoffMs() {
		return retryBackoffMs;
	}

	public void setRetryBackoffMs(
			String retryBackoffMs ) {
		this.retryBackoffMs = retryBackoffMs;
	}
}
