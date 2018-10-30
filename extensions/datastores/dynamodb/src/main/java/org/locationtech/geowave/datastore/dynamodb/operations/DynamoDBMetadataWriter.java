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
package org.locationtech.geowave.datastore.dynamodb.operations;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;

public class DynamoDBMetadataWriter implements
		MetadataWriter
{
	private final static Logger LOGGER = LoggerFactory.getLogger(DynamoDBMetadataWriter.class);

	final DynamoDBOperations operations;
	private final String tableName;

	public DynamoDBMetadataWriter(
			final DynamoDBOperations operations,
			final String tableName ) {
		this.operations = operations;
		this.tableName = tableName;
	}

	@Override
	public void close()
			throws Exception {}

	@Override
	public void write(
			final GeoWaveMetadata metadata ) {
		final Map<String, AttributeValue> map = new HashMap<>();
		map.put(
				DynamoDBOperations.METADATA_PRIMARY_ID_KEY,
				new AttributeValue().withB(ByteBuffer.wrap(metadata.getPrimaryId())));

		if (metadata.getSecondaryId() != null) {
			map.put(
					DynamoDBOperations.METADATA_SECONDARY_ID_KEY,
					new AttributeValue().withB(ByteBuffer.wrap(metadata.getSecondaryId())));
			if (metadata.getVisibility() != null && metadata.getVisibility().length > 0) {
				map.put(
						DynamoDBOperations.METADATA_VISIBILITY_KEY,
						new AttributeValue().withB(ByteBuffer.wrap(metadata.getVisibility())));
			}
		}

		map.put(
				DynamoDBOperations.METADATA_TIMESTAMP_KEY,
				new AttributeValue().withN(Long.toString(System.currentTimeMillis())));
		map.put(
				DynamoDBOperations.METADATA_VALUE_KEY,
				new AttributeValue().withB(ByteBuffer.wrap(metadata.getValue())));

		try {
			operations.getClient().putItem(
					new PutItemRequest(
							tableName,
							map));
		}
		catch (final Exception e) {
			LOGGER.error(
					"Error writing metadata",
					e);
		}
	}

	@Override
	public void flush() {}

}
