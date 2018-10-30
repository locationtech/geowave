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
package org.locationtech.geowave.datastore.cassandra.operations;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraWriter implements
		RowWriter
{

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraWriter.class);
	private final Object MUTEX = new Object();
	private BatchedWrite batchedWrite = null;
	private final CassandraOperations operations;
	private final String tableName;

	public CassandraWriter(
			final String tableName,
			final CassandraOperations operations ) {
		this.tableName = tableName;
		this.operations = operations;
	}

	@Override
	public void close()
			throws Exception {
		flush();
	}

	@Override
	public void write(
			GeoWaveRow[] rows ) {
		for (final GeoWaveRow row : rows) {
			write(row);
		}
	}

	@Override
	public void write(
			GeoWaveRow row ) {
		synchronized (MUTEX) {
			if (batchedWrite == null) {
				batchedWrite = operations.getBatchedWrite(tableName);
			}
			batchedWrite.insert(row);
		}
	}

	@Override
	public void flush() {
		synchronized (MUTEX) {
			if (batchedWrite != null) {
				try {
					batchedWrite.close();
				}
				catch (final Exception e) {
					LOGGER.warn(
							"Unable to close batched write",
							e);
				}
			}
		}
	}

}
