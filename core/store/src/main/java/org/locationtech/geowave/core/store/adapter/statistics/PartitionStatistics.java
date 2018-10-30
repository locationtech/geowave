/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.adapter.statistics;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

/**
 * This class is responsible for maintaining all unique Partition IDs that are
 * being used within a dataset.
 *
 * @param <T>
 *            The type of the row to keep statistics on
 */
public class PartitionStatistics<T> extends
		AbstractDataStatistics<T, Set<ByteArray>, IndexStatisticsQueryBuilder<Set<ByteArray>>>
{
	public static final IndexStatisticsType<Set<ByteArray>> STATS_TYPE = new IndexStatisticsType<>(
			"PARTITIONS");
	private Set<ByteArray> partitions = new HashSet<>();

	public PartitionStatistics() {
		super();
	}

	public PartitionStatistics(
			final Short internalDataAdapterId,
			final String indexName ) {
		super(
				internalDataAdapterId,
				STATS_TYPE,
				indexName);
	}

	@Override
	public InternalDataStatistics<T, Set<ByteArray>, IndexStatisticsQueryBuilder<Set<ByteArray>>> duplicate() {
		return new PartitionStatistics<>(
				adapterId,
				extendedId); // indexId
	}

	public Set<ByteArray> getPartitionKeys() {
		return partitions;
	}

	@Override
	public void merge(
			final Mergeable mergeable ) {
		if (mergeable instanceof PartitionStatistics) {
			partitions.addAll(((PartitionStatistics<?>) mergeable).partitions);
		}
	}

	@Override
	public byte[] toBinary() {
		if (!partitions.isEmpty()) {
			// we know each partition is constant size, so start with the size
			// of the partition keys
			final ByteArray first = partitions.iterator().next();
			if ((first != null) && (first.getBytes() != null)) {
				final ByteBuffer buffer = super.binaryBuffer((first.getBytes().length * partitions.size()) + 1);
				buffer.put((byte) first.getBytes().length);
				for (final ByteArray e : partitions) {
					buffer.put(e.getBytes());
				}
				return buffer.array();
			}
		}
		return super.binaryBuffer(
				0).array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		partitions = new HashSet<>();
		if (buffer.remaining() > 0) {
			final int partitionKeySize = unsignedToBytes(buffer.get());
			if (partitionKeySize > 0) {
				final int numPartitions = buffer.remaining() / partitionKeySize;
				for (int i = 0; i < numPartitions; i++) {
					final byte[] partition = new byte[partitionKeySize];
					buffer.get(partition);
					partitions.add(new ByteArray(
							partition));
				}
			}
		}
	}

	public static int unsignedToBytes(
			final byte b ) {
		return b & 0xFF;
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		for (final GeoWaveRow kv : kvs) {
			add(getPartitionKey(kv.getPartitionKey()));

		}
	}

	protected static ByteArray getPartitionKey(
			final byte[] partitionBytes ) {
		return ((partitionBytes == null) || (partitionBytes.length == 0)) ? null : new ByteArray(
				partitionBytes);
	}

	protected void add(
			final ByteArray partition ) {
		partitions.add(partition);
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer(
				statisticsType.getString()).append(
				" (").append(
				extendedId).append(
				")").append(
				"=");
		if (!partitions.isEmpty()) {
			for (final ByteArray p : partitions) {
				if ((p == null) || (p.getBytes() == null)) {
					buffer.append("null,");
				}
				else {
					buffer.append(
							p.getHexString()).append(
							",");
				}
			}
			buffer.deleteCharAt(buffer.length() - 1);
		}
		else {
			buffer.append("none");
		}
		return buffer.toString();
	}

	@Override
	public Set<ByteArray> getResult() {
		return partitions;
	}

	@Override
	protected String resultsName() {
		return "partitions";
	}

	/**
	 * Convert Row Range Numeric statistics to a JSON object
	 */

	@Override
	protected Object resultsValue() {
		final Collection<Map<String, String>> partitionsArray = new ArrayList<>();
		for (final ByteArray p : partitions) {
			final Map<String, String> partition = new HashMap<>();

			if ((p == null) || (p.getBytes() == null)) {
				partition.put(
						"partition",
						"null");
			}
			else {
				partition.put(
						"partition",
						p.getHexString());
			}
			partitionsArray.add(partition);
		}
		return partitionsArray;
	}
}
