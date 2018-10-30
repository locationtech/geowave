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

import java.util.Map;
import java.util.Map.Entry;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.datastore.dynamodb.DynamoDBRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

public class DynamoDBDeleter implements
		RowDeleter
{
	private final DynamoDBOperations operations;
	private final String tableName;

	public DynamoDBDeleter(
			final DynamoDBOperations operations,
			final String qualifiedTableName ) {
		this.operations = operations;
		tableName = qualifiedTableName;
	}

	@Override
	public void close()
			throws Exception {}

	@Override
	public void delete(
			final GeoWaveRow row ) {
		final DynamoDBRow dynRow = (DynamoDBRow) row;

		for (final Map<String, AttributeValue> attributeMappings : dynRow.getAttributeMapping()) {
			operations.getClient().deleteItem(
					tableName,
					Maps.filterEntries(
							attributeMappings,
							new Predicate<Entry<String, AttributeValue>>() {
								@Override
								public boolean apply(
										final Entry<String, AttributeValue> input ) {
									return DynamoDBRow.GW_PARTITION_ID_KEY.equals(input.getKey())
											|| DynamoDBRow.GW_RANGE_KEY.equals(input.getKey());
								}
							}));
		}
	}

	@Override
	public void flush() {
		// Do nothing, delete is done immediately.
	}

}
