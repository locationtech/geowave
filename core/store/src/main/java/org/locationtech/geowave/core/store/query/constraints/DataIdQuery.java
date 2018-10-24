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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.DataIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class DataIdQuery implements
		QueryConstraints
{
	private ByteArray[] dataIds;

	public DataIdQuery() {}

	public DataIdQuery(
			final ByteArray dataId ) {
		dataIds = new ByteArray[] {
			dataId
		};
	}

	public DataIdQuery(
			final ByteArray[] dataIds ) {
		this.dataIds = dataIds;
	}

	public ByteArray[] getDataIds() {
		return dataIds;
	}

	@Override
	public List<QueryFilter> createFilters(
			final Index index ) {
		final List<QueryFilter> filters = new ArrayList<>();
		filters.add(new DataIdQueryFilter(
				dataIds));
		return filters;
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			final Index index ) {
		return Collections.emptyList();
	}

	@Override
	public byte[] toBinary() {
		final Stream<byte[]> arrays = Arrays
				.stream(
						dataIds)
				.map(
						i -> i.getBytes());
		final int length = arrays
				.map(
						i -> i.length)
				.reduce(
						4,
						Integer::sum);
		final ByteBuffer buf = ByteBuffer
				.allocate(
						length);
		arrays
				.forEach(
						i -> buf
								.putInt(
										i.length)
								.put(
										i));
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int length = buf.getInt();
		final ByteArray[] dataIds = new ByteArray[length];
		for (int i = 0; i < length; i++) {
			final int iLength = buf.getInt();
			final byte[] dataIdBinary = new byte[iLength];
			buf.get(dataIdBinary);
			dataIds[i] = new ByteArray(
					dataIdBinary);
		}
		this.dataIds = dataIds;
	}

}
