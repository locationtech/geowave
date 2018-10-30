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
import org.locationtech.geowave.core.store.query.filter.InsertionIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class InsertionIdQuery implements
		QueryConstraints
{
	private ByteArray partitionKey;
	private ByteArray sortKey;
	private ByteArray dataId;

	public InsertionIdQuery() {}

	public InsertionIdQuery(
			final ByteArray partitionKey,
			final ByteArray sortKey,
			final ByteArray dataId ) {
		this.partitionKey = partitionKey;
		this.sortKey = sortKey;
		this.dataId = dataId;
	}

	public ByteArray getPartitionKey() {
		return partitionKey;
	}

	public ByteArray getSortKey() {
		return sortKey;
	}

	public ByteArray getDataId() {
		return dataId;
	}

	@Override
	public List<QueryFilter> createFilters(
			final Index index ) {
		final List<QueryFilter> filters = new ArrayList<>();
		filters.add(new InsertionIdQueryFilter(
				partitionKey,
				sortKey,
				dataId));
		return filters;
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			final Index index ) {
		return Collections.emptyList();
	}

	@Override
	public byte[] toBinary() {
		byte[] sortKeyBinary, partitionKeyBinary, dataIdBinary;
		if (partitionKey != null) {
			partitionKeyBinary = partitionKey.getBytes();
		}
		else {
			partitionKeyBinary = new byte[0];
		}
		if (sortKey != null) {
			sortKeyBinary = sortKey.getBytes();
		}
		else {
			sortKeyBinary = new byte[0];
		}
		if (dataId != null) {
			dataIdBinary = dataId.getBytes();
		}
		else {
			dataIdBinary = new byte[0];
		}
		final ByteBuffer buf = ByteBuffer.allocate(8 + sortKeyBinary.length + partitionKeyBinary.length);
		buf.putInt(partitionKeyBinary.length);
		buf.put(partitionKeyBinary);
		buf.putInt(sortKeyBinary.length);
		buf.put(sortKeyBinary);
		buf.put(dataIdBinary);
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
		final byte[] sortKeyBinary = new byte[buf.getInt()];
		if (sortKeyBinary.length == 0) {
			sortKey = null;
		}
		else {
			buf.get(sortKeyBinary);
			sortKey = new ByteArray(
					sortKeyBinary);
		}
		final byte[] dataIdBinary = new byte[bytes.length - 8 - sortKeyBinary.length - partitionKeyBinary.length];
		if (dataIdBinary.length == 0) {
			dataId = null;
		}
		else {
			buf.get(dataIdBinary);
			dataId = new ByteArray(
					dataIdBinary);
		}
	}

}
