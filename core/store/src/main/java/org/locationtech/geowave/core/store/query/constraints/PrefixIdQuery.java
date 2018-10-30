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
package org.locationtech.geowave.core.store.query.constraints;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.PrefixIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class PrefixIdQuery implements
		QueryConstraints
{
	private ByteArray sortKeyPrefix;
	private ByteArray partitionKey;

	public PrefixIdQuery() {}

	public PrefixIdQuery(
			final ByteArray partitionKey,
			final ByteArray sortKeyPrefix ) {
		this.partitionKey = partitionKey;
		this.sortKeyPrefix = sortKeyPrefix;
	}

	public ByteArray getPartitionKey() {
		return partitionKey;
	}

	public ByteArray getSortKeyPrefix() {
		return sortKeyPrefix;
	}

	@Override
	public List<QueryFilter> createFilters(
			final Index index ) {
		final List<QueryFilter> filters = new ArrayList<>();
		filters.add(new PrefixIdQueryFilter(
				partitionKey,
				sortKeyPrefix));
		return filters;
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			final Index index ) {
		return Collections.emptyList();
	}

	@Override
	public byte[] toBinary() {
		byte[] sortKeyPrefixBinary, partitionKeyBinary;
		if (partitionKey != null) {
			partitionKeyBinary = partitionKey.getBytes();
		}
		else {
			partitionKeyBinary = new byte[0];
		}
		if (sortKeyPrefix != null) {
			sortKeyPrefixBinary = sortKeyPrefix.getBytes();
		}
		else {
			sortKeyPrefixBinary = new byte[0];
		}
		final ByteBuffer buf = ByteBuffer.allocate(4 + sortKeyPrefixBinary.length + partitionKeyBinary.length);
		buf.putInt(partitionKeyBinary.length);
		buf.put(partitionKeyBinary);
		buf.put(sortKeyPrefixBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final byte[] partitionKeyBinary = new byte[buf.getInt()];
		if (partitionKeyBinary.length == 0) {
			partitionKey = null;
		}
		else {
			buf.get(partitionKeyBinary);
			partitionKey = new ByteArray(
					partitionKeyBinary);
		}
		final byte[] sortKeyPrefixBinary = new byte[bytes.length - 4 - partitionKeyBinary.length];
		if (sortKeyPrefixBinary.length == 0) {
			sortKeyPrefix = null;
		}
		else {
			buf.get(sortKeyPrefixBinary);
			sortKeyPrefix = new ByteArray(
					sortKeyPrefixBinary);
		}
	}

}
