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
package mil.nga.giat.geowave.core.geotime;

import mil.nga.giat.geowave.core.geotime.GeometryUtilsTest.ExampleNumericIndexStrategy;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.geotime.store.data.PersistenceEncodingTest.GeoObjDataAdapter;
import mil.nga.giat.geowave.core.geotime.store.data.PersistenceEncodingTest.TimeFieldHandler;
import mil.nga.giat.geowave.core.geotime.store.data.PersistenceEncodingTest.TimeRangeFieldHandler;
import mil.nga.giat.geowave.core.geotime.store.dimension.LatitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.LongitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.SpatialArrayField;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time.TimeRange;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time.Timestamp;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeArrayField;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.persist.PersistableRegistrySpi;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;

public class TestGeoTimePersistableRegistry implements
		PersistableRegistrySpi
{

	@Override
	public PersistableIdAndConstructor[] getSupportedPersistables() {
		return new PersistableIdAndConstructor[] {
			new PersistableIdAndConstructor(
					(short) 10300,
					ExampleNumericIndexStrategy::new),
			new PersistableIdAndConstructor(
					(short) 10301,
					GeoObjDataAdapter::new),
			new PersistableIdAndConstructor(
					(short) 10302,
					TimeFieldHandler::new),
			new PersistableIdAndConstructor(
					(short) 10303,
					TimeRangeFieldHandler::new),
		};
	}
}
