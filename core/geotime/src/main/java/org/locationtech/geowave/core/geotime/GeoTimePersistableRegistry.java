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
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSBoundedSpatialDimension;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSSpatialField;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSUnboundedSpatialDimension;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSUnboundedSpatialDimensionX;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSUnboundedSpatialDimensionY;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCrsIndexModel;
import org.locationtech.geowave.core.geotime.store.dimension.LatitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.LongitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialArrayField;
import org.locationtech.geowave.core.geotime.store.dimension.Time.TimeRange;
import org.locationtech.geowave.core.geotime.store.dimension.Time.Timestamp;
import org.locationtech.geowave.core.geotime.store.dimension.TimeArrayField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.geotime.store.query.IndexOnlySpatialQuery;
import org.locationtech.geowave.core.geotime.store.query.OptimalCQLQuery;
import org.locationtech.geowave.core.geotime.store.query.SpatialQuery;
import org.locationtech.geowave.core.geotime.store.query.SpatialTemporalQuery;
import org.locationtech.geowave.core.geotime.store.query.TemporalQuery;
import org.locationtech.geowave.core.geotime.store.query.aggregate.CommonIndexBoundingBoxAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.CommonIndexTimeRangeAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.geotime.store.query.aggregate.OptimalVectorBoundingBoxAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.OptimalVectorTimeRangeAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.VectorBoundingBoxAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.VectorTimeRangeAggregation;
import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter;
import org.locationtech.geowave.core.index.dimension.bin.BasicBinningStrategy;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;

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
					CustomCRSBoundedSpatialDimension::new),
			new PersistableIdAndConstructor(
					(short) 315,
					CustomCrsIndexModel::new),
			new PersistableIdAndConstructor(
					(short) 316,
					IndexOnlySpatialQuery::new),
			new PersistableIdAndConstructor(
					(short) 317,
					SpatialTemporalQuery::new),
			new PersistableIdAndConstructor(
					(short) 318,
					TemporalQuery::new),
			new PersistableIdAndConstructor(
					(short) 319,
					CustomCRSUnboundedSpatialDimension::new),
			new PersistableIdAndConstructor(
					(short) 320,
					BasicBinningStrategy::new),
			new PersistableIdAndConstructor(
					(short) 321,
					CustomCRSUnboundedSpatialDimensionX::new),
			new PersistableIdAndConstructor(
					(short) 322,
					CustomCRSUnboundedSpatialDimensionY::new),
			new PersistableIdAndConstructor(
					(short) 323,
					VectorTimeRangeAggregation::new),
			new PersistableIdAndConstructor(
					(short) 324,
					CommonIndexTimeRangeAggregation::new),
			new PersistableIdAndConstructor(
					(short) 325,
					FieldNameParam::new),
			new PersistableIdAndConstructor(
					(short) 326,
					OptimalVectorTimeRangeAggregation::new),
			new PersistableIdAndConstructor(
					(short) 327,
					VectorBoundingBoxAggregation::new),
			new PersistableIdAndConstructor(
					(short) 328,
					CommonIndexBoundingBoxAggregation::new),
			new PersistableIdAndConstructor(
					(short) 329,
					OptimalVectorBoundingBoxAggregation::new),
			new PersistableIdAndConstructor(
					(short) 330,
					OptimalCQLQuery::new),
		};
	}
}
