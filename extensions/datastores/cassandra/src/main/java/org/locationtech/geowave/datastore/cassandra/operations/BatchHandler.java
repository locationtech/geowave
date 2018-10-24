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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class BatchHandler
{
	protected final Session session;
	private Type type = Type.UNLOGGED;
	protected final Map<ByteArray, BatchStatement> batches = new HashMap<>();

	public BatchHandler(
			final Session session ) {
		this.session = session;
	}

	protected BatchStatement addStatement(
			final GeoWaveRow row,
			final Statement statement ) {
		ByteArray partition = new ByteArray(
				row.getPartitionKey());
		BatchStatement tokenBatch = batches.get(partition);

		if (tokenBatch == null) {
			tokenBatch = new BatchStatement(
					type);

			batches.put(
					partition,
					tokenBatch);
		}
		synchronized (tokenBatch) {
			tokenBatch.add(statement);
		}
		return tokenBatch;
	}
}
