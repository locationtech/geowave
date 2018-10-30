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
import java.util.Iterator;
import java.util.Map;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.util.StatisticsRowIterator;
import org.locationtech.geowave.datastore.dynamodb.util.DynamoDBUtils;
import org.locationtech.geowave.datastore.dynamodb.util.LazyPaginatedQuery;
import org.locationtech.geowave.datastore.dynamodb.util.LazyPaginatedScan;
import org.locationtech.geowave.datastore.dynamodb.util.DynamoDBUtils.NoopClosableIteratorWrapper;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.collect.Iterators;

public class DynamoDBMetadataReader implements
		MetadataReader
{
	private final DynamoDBOperations operations;
	private final MetadataType metadataType;

	public DynamoDBMetadataReader(
			final DynamoDBOperations operations,
			final MetadataType metadataType ) {
		this.operations = operations;
		this.metadataType = metadataType;
	}

	@Override
	public CloseableIterator<GeoWaveMetadata> query(
			final MetadataQuery query ) {
		final String tableName = operations.getMetadataTableName(metadataType);

		if (query.hasPrimaryId()) {
			final QueryRequest queryRequest = new QueryRequest(
					tableName);

			if (query.hasSecondaryId()) {
				queryRequest.withFilterExpression(
						DynamoDBOperations.METADATA_SECONDARY_ID_KEY + " = :secVal").addExpressionAttributeValuesEntry(
						":secVal",
						new AttributeValue().withB(ByteBuffer.wrap(query.getSecondaryId())));
			}
			queryRequest.withKeyConditionExpression(
					DynamoDBOperations.METADATA_PRIMARY_ID_KEY + " = :priVal").addExpressionAttributeValuesEntry(
					":priVal",
					new AttributeValue().withB(ByteBuffer.wrap(query.getPrimaryId())));

			final QueryResult queryResult = operations.getClient().query(
					queryRequest);

			if (metadataType == MetadataType.STATS) {
				return getStatisticsIterator(
						new LazyPaginatedQuery(
								queryResult,
								queryRequest,
								operations.getClient()), query.getAuthorizations());
			}

			return new CloseableIteratorWrapper<>(
					new NoopClosableIteratorWrapper(),
					Iterators.transform(
							queryResult.getItems().iterator(),				
							 result -> new GeoWaveMetadata(
											DynamoDBUtils.getPrimaryId(result),
											DynamoDBUtils.getSecondaryId(result),
											null,
											DynamoDBUtils.getValue(result))));

		}

		final ScanRequest scan = new ScanRequest(
				tableName);
		if (query.hasSecondaryId()) {
			scan.addScanFilterEntry(
					DynamoDBOperations.METADATA_SECONDARY_ID_KEY,
					new Condition()
							.withAttributeValueList(
									new AttributeValue().withB(ByteBuffer.wrap(query.getSecondaryId())))
							.withComparisonOperator(
									ComparisonOperator.EQ));
		}
		final ScanResult scanResult = operations.getClient().scan(
				scan);

		if (metadataType == MetadataType.STATS) {
			return getStatisticsIterator(
					new LazyPaginatedScan(
							scanResult,
							scan,
							operations.getClient()), query.getAuthorizations());
		}

		return new CloseableIteratorWrapper<>(
				new NoopClosableIteratorWrapper(),
				Iterators.transform(
						scanResult.getItems().iterator(),
						new com.google.common.base.Function<Map<String, AttributeValue>, GeoWaveMetadata>() {
							@Override
							public GeoWaveMetadata apply(
									final Map<String, AttributeValue> result ) {

								return new GeoWaveMetadata(
										DynamoDBUtils.getPrimaryId(result),
										DynamoDBUtils.getSecondaryId(result),
										null,
										DynamoDBUtils.getValue(result));
							}
						}));
	}

	private static CloseableIterator<GeoWaveMetadata> getStatisticsIterator(
			final Iterator<Map<String, AttributeValue>> resultIterator,
			String... authorizations) {
		return new StatisticsRowIterator(
				new CloseableIterator.Wrapper<GeoWaveMetadata>(Iterators.transform(resultIterator,result -> new GeoWaveMetadata(
								DynamoDBUtils.getPrimaryId(result),
								DynamoDBUtils.getSecondaryId(result),
								DynamoDBUtils.getVisibility(result),
								DynamoDBUtils.getValue(result)))), authorizations);
	}
}
