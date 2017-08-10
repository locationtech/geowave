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
package mil.nga.giat.geowave.datastore.accumulo;

import mil.nga.giat.geowave.core.index.persist.PersistableRegistrySpi;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest.GeoBoundingBoxStatistics;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreStatsTest.TestGeometryAdapter;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOptionsTest.AnotherAdapter;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRangeQueryTest;

public class TestAccumuloPersistableRegistry implements
		PersistableRegistrySpi
{
	@Override
	public PersistableIdAndConstructor[] getSupportedPersistables() {
		return new PersistableIdAndConstructor[] {
			new PersistableIdAndConstructor(
					(short) 10250,
					GeoBoundingBoxStatistics::new),
			new PersistableIdAndConstructor(
					(short) 10251,
					TestGeometryAdapter::new),
			new PersistableIdAndConstructor(
					(short) 10252,
					AccumuloOptionsTest.TestGeometryAdapter::new),
			new PersistableIdAndConstructor(
					(short) 10253,
					AnotherAdapter::new),
			new PersistableIdAndConstructor(
					(short) 10254,
					AccumuloRangeQueryTest.TestGeometryAdapter::new),
		};
	}
}
