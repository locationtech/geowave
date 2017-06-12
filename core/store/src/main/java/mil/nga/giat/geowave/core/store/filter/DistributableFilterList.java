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
package mil.nga.giat.geowave.core.store.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;

/**
 * This class wraps a list of distributable filters into a single distributable
 * filter such that the list is persisted in its original order and if any one
 * filter fails this class will fail acceptance.
 *
 */
public class DistributableFilterList extends
		FilterList<DistributableQueryFilter> implements
		DistributableQueryFilter
{
	public DistributableFilterList() {
		super();
	}

	public DistributableFilterList(
			final List<DistributableQueryFilter> filters ) {
		super(
				filters);
	}

	public DistributableFilterList(
			final boolean logicalAnd,
			final List<DistributableQueryFilter> filters ) {
		super(
				logicalAnd,
				filters);
	}

	@Override
	public byte[] toBinary() {
		int byteBufferLength = 8;
		final List<byte[]> filterBinaries = new ArrayList<byte[]>(
				filters.size());
		for (final DistributableQueryFilter filter : filters) {
			final byte[] filterBinary = PersistenceUtils.toBinary(filter);
			byteBufferLength += (4 + filterBinary.length);
			filterBinaries.add(filterBinary);
		}
		final ByteBuffer buf = ByteBuffer.allocate(byteBufferLength);
		buf.putInt(logicalAnd ? 1 : 0);
		buf.putInt(filters.size());
		for (final byte[] filterBinary : filterBinaries) {
			buf.putInt(filterBinary.length);
			buf.put(filterBinary);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		logicalAnd = buf.getInt() > 0;
		final int numFilters = buf.getInt();
		filters = new ArrayList<DistributableQueryFilter>(
				numFilters);
		for (int i = 0; i < numFilters; i++) {
			final byte[] filter = new byte[buf.getInt()];
			buf.get(filter);
			filters.add((DistributableQueryFilter) PersistenceUtils.fromBinary(filter));
		}
	}
}
