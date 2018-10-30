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
package org.locationtech.geowave.datastore.bigtable;

import org.locationtech.geowave.core.store.StoreFactoryHelper;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreFactory;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.datastore.bigtable.operations.config.BigTableOptions;

/**
 * Big Table needs to safe guard its column qualifiers against control
 * characters (rather than use the default byte array from shorts as the cq)
 */
public class BigTableDataStatisticsStoreFactory extends
		DataStatisticsStoreFactory
{
	public BigTableDataStatisticsStoreFactory(
			final String typeName,
			final String description,
			final StoreFactoryHelper helper ) {
		super(
				typeName,
				description,
				helper);
	}

	@Override
	public DataStatisticsStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof BigTableOptions)) {
			throw new AssertionError(
					"Expected " + BigTableOptions.class.getSimpleName());
		}

		final DataStoreOperations bigtableOperations = helper.createOperations(options);

		return new BigTableDataStatisticsStore(
				bigtableOperations,
				((BigTableOptions) options).getHBaseOptions());
	}
}
