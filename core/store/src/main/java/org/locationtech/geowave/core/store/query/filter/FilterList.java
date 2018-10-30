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
package org.locationtech.geowave.core.store.query.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

/**
 * This class wraps a list of filters into a single filter such that if any one
 * filter fails this class will fail acceptance.
 * 
 * @param <T>
 */
public class FilterList implements
		QueryFilter
{
	protected List<QueryFilter> filters;
	protected boolean logicalAnd = true;

	public FilterList() {}

	protected FilterList(
			boolean logicalAnd ) {
		this.logicalAnd = logicalAnd;
	}

	public FilterList(
			final List<QueryFilter> filters ) {
		this.filters = filters;
	}

	public FilterList(
			boolean logicalAnd,
			final List<QueryFilter> filters ) {
		this.logicalAnd = logicalAnd;
		this.filters = filters;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding<?> entry ) {
		if (filters == null) return true;
		for (final QueryFilter filter : filters) {
			final boolean ok = filter.accept(
					indexModel,
					entry);
			if (!ok && logicalAnd) return false;
			if (ok && !logicalAnd) return true;

		}
		return logicalAnd;
	}

	@Override
	public byte[] toBinary() {
		int byteBufferLength = 8;
		final List<byte[]> filterBinaries = new ArrayList<byte[]>(
				filters.size());
		for (final QueryFilter filter : filters) {
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
		filters = new ArrayList<>(
				numFilters);
		for (int i = 0; i < numFilters; i++) {
			final byte[] filter = new byte[buf.getInt()];
			buf.get(filter);
			filters.add((QueryFilter) PersistenceUtils.fromBinary(filter));
		}
	}
}
