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
package mil.nga.giat.geowave.datastore.bigtable;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;

/**
 * Big Table needs to safe guard its column qualifiers against control
 * characters (rather than use the default byte array from shorts as the cq)
 */
public class BigTableDataStatisticsStore extends
		DataStatisticsStoreImpl
{

	public BigTableDataStatisticsStore(
			final DataStoreOperations operations,
			final DataStoreOptions options ) {
		super(
				operations,
				options);
	}

	@Override
	protected ByteArrayId shortToByteArrayId(
			final short internalAdapterId ) {
		return new ByteArrayId(
				ByteArrayUtils.shortToString(internalAdapterId));
	}

	@Override
	protected short byteArrayToShort(
			final byte[] bytes ) {
		// make sure we use the same String encoding as decoding byt using the
		// ByteArrayId constructor
		return ByteArrayUtils.shortFromString(new ByteArrayId(
				bytes).getString());
	}

}
