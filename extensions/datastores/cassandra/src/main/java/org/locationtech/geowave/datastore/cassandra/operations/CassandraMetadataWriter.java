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

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataWriter;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CassandraMetadataWriter implements
		MetadataWriter
{
	protected static final String PRIMARY_ID_KEY = "I";
	protected static final String SECONDARY_ID_KEY = "S";
	// serves as unique ID for instances where primary+secondary are repeated
	protected static final String TIMESTAMP_ID_KEY = "T";
	protected static final String VISIBILITY_KEY = "A";
	protected static final String VALUE_KEY = "V";

	private final CassandraOperations operations;
	private final String tableName;

	public CassandraMetadataWriter(
			final CassandraOperations operations,
			final String tableName ) {
		this.operations = operations;
		this.tableName = tableName;
	}

	@Override
	public void close()
			throws Exception {

	}

	@Override
	public void write(
			final GeoWaveMetadata metadata ) {
		final Insert insert = operations.getInsert(tableName);
		insert.value(
				PRIMARY_ID_KEY,
				ByteBuffer.wrap(metadata.getPrimaryId()));
		if (metadata.getSecondaryId() != null) {
			insert.value(
					SECONDARY_ID_KEY,
					ByteBuffer.wrap(metadata.getSecondaryId()));
			insert.value(
					TIMESTAMP_ID_KEY,
					QueryBuilder.now());
			if (metadata.getVisibility() != null && metadata.getVisibility().length > 0) {
				insert.value(
						VISIBILITY_KEY,
						ByteBuffer.wrap(metadata.getVisibility()));
			}
		}
		insert.value(
				VALUE_KEY,
				ByteBuffer.wrap(metadata.getValue()));
		operations.getSession().execute(
				insert);
	}

	@Override
	public void flush() {

	}

}
