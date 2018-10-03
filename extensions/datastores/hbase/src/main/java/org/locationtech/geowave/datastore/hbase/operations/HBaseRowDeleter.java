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
package org.locationtech.geowave.datastore.hbase.operations;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseRowDeleter implements
		RowDeleter
{
	private static Logger LOGGER = LoggerFactory.getLogger(HBaseRowDeleter.class);
	private final BufferedMutator deleter;
	protected Set<ByteArrayId> duplicateRowTracker = new HashSet<>();

	public HBaseRowDeleter(
			final BufferedMutator deleter ) {
		this.deleter = deleter;
	}

	@Override
	public void close() {
		try {
			if (deleter != null) {
				deleter.close();
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to close BufferedMutator",
					e);
		}
	}

	@Override
	public void delete(
			final GeoWaveRow row ) {

		byte[] rowBytes = GeoWaveKey.getCompositeId(row);
		final Delete delete = new Delete(
				rowBytes);
		// we use a hashset of row IDs so that we can retain multiple versions
		// (otherwise timestamps will be applied on the server side in
		// batches and if the same row exists within a batch we will not
		// retain multiple versions)
		try {
			synchronized (duplicateRowTracker) {
				final ByteArrayId rowId = new ByteArrayId(
						rowBytes);
				if (!duplicateRowTracker.add(rowId)) {
					deleter.flush();
					duplicateRowTracker.clear();
					duplicateRowTracker.add(rowId);
				}
			}
			deleter.mutate(delete);
		}
		catch (IOException e) {
			LOGGER.warn(
					"Unable to delete row",
					e);
		}
	}
}