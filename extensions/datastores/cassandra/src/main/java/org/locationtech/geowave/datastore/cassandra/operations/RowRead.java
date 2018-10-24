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

import java.nio.ByteBuffer;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.datastore.cassandra.CassandraRow;
import org.locationtech.geowave.datastore.cassandra.CassandraRow.CassandraField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.TypeCodec;

public class RowRead
{
	private final static Logger LOGGER = LoggerFactory.getLogger(RowRead.class);
	private final CassandraOperations operations;
	private final PreparedStatement preparedRead;
	private final short internalAdapterId;
	private final byte[] partitionKey;
	private final byte[] sortKey;

	protected RowRead(
			final PreparedStatement preparedRead,
			final CassandraOperations operations,
			final byte[] partitionKey,
			final byte[] sortKey,
			final Short internalAdapterId ) {
		this.preparedRead = preparedRead;
		this.operations = operations;
		this.partitionKey = partitionKey;
		this.sortKey = sortKey;
		this.internalAdapterId = internalAdapterId;
	}

	public CassandraRow result() {
		if ((partitionKey != null) && (sortKey != null)) {
			final BoundStatement boundRead = new BoundStatement(
					preparedRead);
			boundRead.set(
					CassandraField.GW_SORT_KEY.getBindMarkerName(),
					ByteBuffer.wrap(sortKey),
					ByteBuffer.class);
			boundRead.set(
					CassandraField.GW_ADAPTER_ID_KEY.getBindMarkerName(),
					internalAdapterId,
					TypeCodec.smallInt());
			boundRead.set(
					CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName(),
					ByteBuffer.wrap(partitionKey),
					ByteBuffer.class);
			try (CloseableIterator<CassandraRow> it = operations.executeQuery(boundRead)) {
				if (it.hasNext()) {
					// there should only be one entry with this index
					return it.next();
				}
			}
		}
		return null;
	}
}
