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
package mil.nga.giat.geowave.core.store.filter;

import java.nio.ByteBuffer;
import java.util.Arrays;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public class PrefixIdQueryFilter implements
		DistributableQueryFilter
{
	private byte[] partitionKey;
	private byte[] sortKeyPrefix;

	public PrefixIdQueryFilter() {}

	public PrefixIdQueryFilter(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKeyPrefix ) {
		this.partitionKey = ((partitionKey != null) && (partitionKey.getBytes() != null)) ? partitionKey.getBytes()
				: new byte[0];
		this.sortKeyPrefix = sortKeyPrefix.getBytes();
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding persistenceEncoding ) {
		final ByteArrayId otherPartitionKey = persistenceEncoding.getInsertionPartitionKey();
		final byte[] otherPartitionKeyBytes = ((otherPartitionKey != null) && (otherPartitionKey.getBytes() != null)) ? otherPartitionKey
				.getBytes() : new byte[0];
		final ByteArrayId sortKey = persistenceEncoding.getInsertionSortKey();
		return (Arrays.equals(
				sortKeyPrefix,
				Arrays.copyOf(
						sortKey.getBytes(),
						sortKeyPrefix.length)) && Arrays.equals(
				partitionKey,
				otherPartitionKeyBytes));
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = ByteBuffer.allocate(8 + partitionKey.length + sortKeyPrefix.length);
		buf.putInt(partitionKey.length);
		buf.put(partitionKey);
		buf.putInt(sortKeyPrefix.length);
		buf.put(sortKeyPrefix);

		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		partitionKey = new byte[buf.getInt()];
		buf.get(partitionKey);
		sortKeyPrefix = new byte[buf.getInt()];
		buf.get(sortKeyPrefix);
	}
}
