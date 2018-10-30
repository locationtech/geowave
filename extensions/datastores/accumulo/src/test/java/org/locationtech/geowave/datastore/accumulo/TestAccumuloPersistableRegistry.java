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
package org.locationtech.geowave.datastore.accumulo;

import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;
import org.locationtech.geowave.datastore.accumulo.AccumuloDataStoreStatsTest.GeoBoundingBoxStatistics;
import org.locationtech.geowave.datastore.accumulo.AccumuloDataStoreStatsTest.TestGeometryAdapter;
import org.locationtech.geowave.datastore.accumulo.AccumuloOptionsTest.AnotherAdapter;
import org.locationtech.geowave.datastore.accumulo.query.AccumuloRangeQueryTest;

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
