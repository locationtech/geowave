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
import org.locationtech.geowave.core.store.operations.RowDeleter;

public class CassandraDeleter implements
		RowDeleter
{
	private final CassandraOperations operations;
	private final String tableName;

	public CassandraDeleter(
			final CassandraOperations operations,
			final String tableName ) {
		this.operations = operations;
		this.tableName = tableName;
	}

	@Override
	public void delete(
			final GeoWaveRow row ) {
		operations.deleteRow(
				tableName,
				row);
	}

	@Override
	public void flush() {
		// Do nothing, delete is done immediately.
	}

	@Override
	public void close()
			throws Exception {

	}
}
