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

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public class RowIdQueryFilter implements
		DistributableQueryFilter
{
	private List<ByteArrayId> rowIds;

	public RowIdQueryFilter() {}

	public RowIdQueryFilter(
			final List<ByteArrayId> rowIds ) {
		this.rowIds = rowIds;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding persistenceEncoding ) {
		return rowIds.contains(persistenceEncoding.getIndexInsertionId());
	}

	@Override
	public byte[] toBinary() {
		int size = 4;
		for (final ByteArrayId id : rowIds) {
			size += (id.getBytes().length + 4);
		}
		final ByteBuffer buf = ByteBuffer.allocate(size);
		buf.putInt(rowIds.size());
		for (final ByteArrayId id : rowIds) {
			final byte[] idBytes = id.getBytes();
			buf.putInt(idBytes.length);
			buf.put(idBytes);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int size = buf.getInt();
		rowIds = new ArrayList<ByteArrayId>(
				size);
		for (int i = 0; i < size; i++) {
			final int bsize = buf.getInt();
			final byte[] dataIdBytes = new byte[bsize];
			buf.get(dataIdBytes);
			rowIds.add(new ByteArrayId(
					dataIdBytes));
		}

	}
}
