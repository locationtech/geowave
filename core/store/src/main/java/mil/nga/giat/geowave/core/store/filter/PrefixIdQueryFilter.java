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

import java.util.Arrays;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public class PrefixIdQueryFilter implements
		DistributableQueryFilter
{
	private ByteArrayId rowPrefix;

	public PrefixIdQueryFilter() {}

	public PrefixIdQueryFilter(
			final ByteArrayId rowPrefix ) {
		this.rowPrefix = rowPrefix;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding persistenceEncoding ) {
		ByteArrayId rowId = persistenceEncoding.getIndexInsertionId();
		return (Arrays.equals(
				rowPrefix.getBytes(),
				Arrays.copyOf(
						rowId.getBytes(),
						rowId.getBytes().length)));
	}

	@Override
	public byte[] toBinary() {
		return rowPrefix.getBytes();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		rowPrefix = new ByteArrayId(
				bytes);
	}
}
