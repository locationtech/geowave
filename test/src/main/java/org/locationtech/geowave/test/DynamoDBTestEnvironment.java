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
package org.locationtech.geowave.test;

import org.apache.log4j.Logger;
import org.locationtech.geowave.core.store.GenericStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.datastore.dynamodb.DynamoDBOptions;
import org.locationtech.geowave.datastore.dynamodb.DynamoDBStoreFactoryFamily;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

public class DynamoDBTestEnvironment extends
		StoreTestEnvironment
{
	private static final GenericStoreFactory<DataStore> STORE_FACTORY = new DynamoDBStoreFactoryFamily()
			.getDataStoreFactory();

	private static DynamoDBTestEnvironment singletonInstance = null;

	public static synchronized DynamoDBTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new DynamoDBTestEnvironment();
		}
		return singletonInstance;
	}

	private final static Logger LOGGER = Logger.getLogger(DynamoDBTestEnvironment.class);

	protected DynamoDBLocal dynamoLocal;

	private DynamoDBTestEnvironment() {}

	@Override
	public void setup() {
		// DynamoDB IT's rely on an external dynamo local process
		if (dynamoLocal == null) {
			dynamoLocal = new DynamoDBLocal(
					null); // null uses tmp dir
		}

		// Make sure we clean up any old processes first
		if (dynamoLocal.isRunning()) {
			dynamoLocal.stop();
		}

		if (!dynamoLocal.start()) {
			LOGGER.error("DynamoDB emulator startup failed");
		}
	}

	@Override
	public void tearDown() {
		dynamoLocal.stop();
	}

	@Override
	protected GenericStoreFactory<DataStore> getDataStoreFactory() {
		return STORE_FACTORY;
	}

	@Override
	protected GeoWaveStoreType getStoreType() {
		return GeoWaveStoreType.DYNAMODB;
	}

	@Override
	protected void initOptions(
			final StoreFactoryOptions options ) {
		((DynamoDBOptions) options).setEndpoint("http://localhost:8000");
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {};
	}

}
