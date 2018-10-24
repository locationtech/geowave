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
import java.util.Arrays;
import java.util.Collection;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

public class DataIdQueryFilter implements
		QueryFilter
{
	private Collection<ByteArray> dataIds;

	public DataIdQueryFilter() {}

	public DataIdQueryFilter(
			final ByteArray[] dataIds ) {
		this.dataIds = Arrays.asList(dataIds);
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding persistenceEncoding ) {
		return dataIds.contains(persistenceEncoding.getDataId());
	}

	@Override
	public byte[] toBinary() {
		int size = 4;
		for (final ByteArray id : dataIds) {
			size += (id.getBytes().length + 4);
		}
		final ByteBuffer buf = ByteBuffer.allocate(size);
		buf.putInt(dataIds.size());
		for (final ByteArray id : dataIds) {
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
		dataIds = new ArrayList<>(
				size);
		for (int i = 0; i < size; i++) {
			final int bsize = buf.getInt();
			final byte[] dataIdBytes = new byte[bsize];
			buf.get(dataIdBytes);
			dataIds.add(new ByteArray(
					dataIdBytes));
		}
	}
}
