/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.store;

import mil.nga.giat.geowave.core.index.persist.PersistableRegistrySpi;
import mil.nga.giat.geowave.core.store.adapter.MockComponents.IntegerRangeDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.MockComponents.MockAbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.MockComponents.MockIndexStrategy;
import mil.nga.giat.geowave.core.store.adapter.MockComponents.TestDimensionField;
import mil.nga.giat.geowave.core.store.adapter.MockComponents.TestIndexModel;
import mil.nga.giat.geowave.core.store.adapter.MockComponents.TestPersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreUtilsTest.TestStringAdapter;
import mil.nga.giat.geowave.core.store.query.BasicQueryTest.ExampleDimensionOne;
import mil.nga.giat.geowave.core.store.query.BasicQueryTest.ExampleNumericIndexStrategy;

public class TestStorePersistableRegistry implements
		PersistableRegistrySpi
{

	@Override
	public PersistableIdAndConstructor[] getSupportedPersistables() {
		return new PersistableIdAndConstructor[] {
			new PersistableIdAndConstructor(
					(short) 10200,
					MockAbstractDataAdapter::new),
			new PersistableIdAndConstructor(
					(short) 10201,
					IntegerRangeDataStatistics::new),
			new PersistableIdAndConstructor(
					(short) 10202,
					TestPersistentIndexFieldHandler::new),
			new PersistableIdAndConstructor(
					(short) 10203,
					TestDimensionField::new),
			new PersistableIdAndConstructor(
					(short) 10204,
					MockIndexStrategy::new),
			new PersistableIdAndConstructor(
					(short) 10205,
					TestIndexModel::new),
			new PersistableIdAndConstructor(
					(short) 10206,
					TestStringAdapter::new),
			new PersistableIdAndConstructor(
					(short) 10207,
					ExampleNumericIndexStrategy::new),
			new PersistableIdAndConstructor(
					(short) 10208,
					ExampleDimensionOne::new)
		};
	}
}
