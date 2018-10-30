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
package org.locationtech.geowave.datastore.bigtable;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;

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
	protected ByteArray shortToByteArrayId(
			final short internalAdapterId ) {
		return new ByteArray(
				ByteArrayUtils.shortToString(internalAdapterId));
	}

	@Override
	protected short byteArrayToShort(
			final byte[] bytes ) {
		// make sure we use the same String encoding as decoding byt using the
		// ByteArrayId constructor
		return ByteArrayUtils.shortFromString(new ByteArray(
				bytes).getString());
	}

}
