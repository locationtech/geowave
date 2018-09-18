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
package org.locationtech.geowave.core.geotime;

import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy;
import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.geotime.store.data.PersistenceEncodingTest.GeoObjDataAdapter;
import org.locationtech.geowave.core.geotime.store.data.PersistenceEncodingTest.TimeFieldHandler;
import org.locationtech.geowave.core.geotime.store.data.PersistenceEncodingTest.TimeRangeFieldHandler;
import org.locationtech.geowave.core.geotime.store.dimension.LatitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.LongitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialArrayField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeArrayField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.geotime.store.dimension.Time.TimeRange;
import org.locationtech.geowave.core.geotime.store.dimension.Time.Timestamp;
import org.locationtech.geowave.core.geotime.store.query.SpatialQuery;
import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter;
import org.locationtech.geowave.core.geotime.util.GeometryUtilsTest.ExampleNumericIndexStrategy;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;
import org.locationtech.geowave.core.store.adapter.PersistentIndexFieldHandler;

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
