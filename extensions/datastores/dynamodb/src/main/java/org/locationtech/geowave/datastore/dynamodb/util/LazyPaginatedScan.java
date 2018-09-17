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
package org.locationtech.geowave.datastore.dynamodb.util;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.collections4.iterators.LazyIteratorChain;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class LazyPaginatedScan extends
		LazyIteratorChain<Map<String, AttributeValue>>
{
	private ScanResult currentResult;
	private final ScanRequest request;
	private final AmazonDynamoDBAsync dynamoDBClient;

	public LazyPaginatedScan(
			final ScanResult currentResult,
			final ScanRequest request,
			final AmazonDynamoDBAsync dynamoDBClient ) {
		this.currentResult = currentResult;
		this.request = request;
		this.dynamoDBClient = dynamoDBClient;
	}

	@Override
	protected Iterator<? extends Map<String, AttributeValue>> nextIterator(
			final int count ) {
		// the first iterator should be the initial results
		if (count == 1) {
			return currentResult.getItems().iterator();
		}
		// subsequent chained iterators will be obtained from dynamoDB
		// pagination
		if ((currentResult.getLastEvaluatedKey() == null) || currentResult.getLastEvaluatedKey().isEmpty()) {
			return null;
		}
		else {
			request.setExclusiveStartKey(currentResult.getLastEvaluatedKey());
			currentResult = dynamoDBClient.scan(request);
			return currentResult.getItems().iterator();
		}
	}
}
