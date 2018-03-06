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

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.geotime.store.dimension.CustomCRSSpatialDimension;
import mil.nga.giat.geowave.core.geotime.store.dimension.CustomCRSSpatialField;
import mil.nga.giat.geowave.core.geotime.store.dimension.CustomCrsIndexModel;
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

public class GeoTimePersistableRegistry implements
		PersistableRegistrySpi
{

	@Override
	public PersistableIdAndConstructor[] getSupportedPersistables() {
		return new PersistableIdAndConstructor[] {
			new PersistableIdAndConstructor(
					(short) 300,
					LatitudeDefinition::new),
			new PersistableIdAndConstructor(
					(short) 301,
					LongitudeDefinition::new),
			new PersistableIdAndConstructor(
					(short) 302,
					TemporalBinningStrategy::new),
			new PersistableIdAndConstructor(
					(short) 303,
					TimeDefinition::new),
			new PersistableIdAndConstructor(
					(short) 304,
					LatitudeField::new),
			new PersistableIdAndConstructor(
					(short) 305,
					LongitudeField::new),
			new PersistableIdAndConstructor(
					(short) 306,
					SpatialArrayField::new),
			new PersistableIdAndConstructor(
					(short) 307,
					TimeRange::new),
			new PersistableIdAndConstructor(
					(short) 308,
					Timestamp::new),
			new PersistableIdAndConstructor(
					(short) 309,
					TimeArrayField::new),
			new PersistableIdAndConstructor(
					(short) 310,
					TimeField::new),
			new PersistableIdAndConstructor(
					(short) 311,
					SpatialQueryFilter::new),
			new PersistableIdAndConstructor(
					(short) 312,
					SpatialQuery::new),
			new PersistableIdAndConstructor(
					(short) 313,
					CustomCRSSpatialField::new),
			new PersistableIdAndConstructor(
					(short) 314,
					CustomCRSSpatialDimension::new),
		    new PersistableIdAndConstructor(
					(short) 315,
					CustomCrsIndexModel::new)
		};
	}
}
