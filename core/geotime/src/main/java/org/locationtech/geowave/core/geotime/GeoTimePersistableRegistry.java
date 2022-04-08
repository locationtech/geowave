/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime;

import org.locationtech.geowave.core.geotime.adapter.SpatialFieldDescriptor;
import org.locationtech.geowave.core.geotime.adapter.TemporalFieldDescriptor;
import org.locationtech.geowave.core.geotime.index.SpatialIndexFilter;
import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.SimpleTimeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.SimpleTimeIndexStrategy;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy;
import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.geotime.store.InternalGeotoolsDataAdapterWrapper;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSBoundedSpatialDimension;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSBoundedSpatialDimensionX;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSBoundedSpatialDimensionY;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSSpatialField;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSUnboundedSpatialDimension;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSUnboundedSpatialDimensionX;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSUnboundedSpatialDimensionY;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCrsIndexModel;
import org.locationtech.geowave.core.geotime.store.dimension.LatitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.LongitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.geotime.store.query.ExplicitCQLQuery;
import org.locationtech.geowave.core.geotime.store.query.ExplicitSpatialQuery;
import org.locationtech.geowave.core.geotime.store.query.ExplicitSpatialTemporalQuery;
import org.locationtech.geowave.core.geotime.store.query.ExplicitTemporalQuery;
import org.locationtech.geowave.core.geotime.store.query.IndexOnlySpatialQuery;
import org.locationtech.geowave.core.geotime.store.query.OptimalCQLQuery;
import org.locationtech.geowave.core.geotime.store.query.SpatialQuery;
import org.locationtech.geowave.core.geotime.store.query.SpatialTemporalQuery;
import org.locationtech.geowave.core.geotime.store.query.TemporalQuery;
import org.locationtech.geowave.core.geotime.store.query.aggregate.CommonIndexBoundingBoxAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.CommonIndexTimeRangeAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.OptimalVectorBoundingBoxAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.OptimalVectorTimeRangeAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.SpatialCommonIndexedBinningStrategy;
import org.locationtech.geowave.core.geotime.store.query.aggregate.SpatialFieldBinningStrategy;
import org.locationtech.geowave.core.geotime.store.query.aggregate.SpatialSimpleFeatureBinningStrategy;
import org.locationtech.geowave.core.geotime.store.query.aggregate.VectorBoundingBoxAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.VectorTimeRangeAggregation;
import org.locationtech.geowave.core.geotime.store.query.filter.CQLQueryFilter;
import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.BBox;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Crosses;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Disjoint;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Intersects;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Overlaps;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.PreparedFilterGeometry;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialContains;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialEqualTo;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialFieldValue;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialLiteral;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialNotEqualTo;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.TextToSpatialExpression;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Touches;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.UnpreparedFilterGeometry;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Within;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.After;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.Before;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.BeforeOrDuring;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.During;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.DuringOrAfter;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalBetween;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalEqualTo;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalFieldValue;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalLiteral;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalNotEqualTo;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TimeOverlaps;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors.TimeDescriptorConfiguration;
import org.locationtech.geowave.core.index.persist.InternalPersistableRegistry;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;

public class GeoTimePersistableRegistry implements
    PersistableRegistrySpi,
    InternalPersistableRegistry {

  // Make sure GeoTools is properly initialized before we do anything
  static {
    GeometryUtils.initClassLoader();
  }

  @Override
  public PersistableIdAndConstructor[] getSupportedPersistables() {
    return new PersistableIdAndConstructor[] {
        new PersistableIdAndConstructor((short) 300, LatitudeDefinition::new),
        new PersistableIdAndConstructor((short) 301, LongitudeDefinition::new),
        new PersistableIdAndConstructor((short) 302, TemporalBinningStrategy::new),
        new PersistableIdAndConstructor((short) 303, TimeDefinition::new),
        // 304 is a legacy class (pre 2.0)
        // 305 is a legacy class (pre 2.0)
        // 306-307 are used by GeotimeRegisteredIndexFieldMappers
        new PersistableIdAndConstructor((short) 308, ExplicitCQLQuery::new),
        new PersistableIdAndConstructor((short) 309, CQLQueryFilter::new),
        new PersistableIdAndConstructor((short) 310, TimeField::new),
        new PersistableIdAndConstructor((short) 311, SpatialQueryFilter::new),
        new PersistableIdAndConstructor((short) 312, ExplicitSpatialQuery::new),
        // 313 is a legacy class (pre 2.0)
        // 523 migrated from adapter-vector, ID is the same to preserve backwards compatibility
        new PersistableIdAndConstructor((short) 523, TimeDescriptorConfiguration::new),
        new PersistableIdAndConstructor((short) 314, CustomCRSBoundedSpatialDimension::new),
        new PersistableIdAndConstructor((short) 315, CustomCrsIndexModel::new),
        new PersistableIdAndConstructor((short) 316, IndexOnlySpatialQuery::new),
        new PersistableIdAndConstructor((short) 317, ExplicitSpatialTemporalQuery::new),
        new PersistableIdAndConstructor((short) 318, ExplicitTemporalQuery::new),
        new PersistableIdAndConstructor((short) 319, CustomCRSUnboundedSpatialDimension::new),
        new PersistableIdAndConstructor((short) 320, SpatialIndexFilter::new),
        new PersistableIdAndConstructor((short) 321, CustomCRSUnboundedSpatialDimensionX::new),
        new PersistableIdAndConstructor((short) 322, CustomCRSUnboundedSpatialDimensionY::new),
        new PersistableIdAndConstructor((short) 323, VectorTimeRangeAggregation::new),
        new PersistableIdAndConstructor((short) 324, CommonIndexTimeRangeAggregation::new),
        new PersistableIdAndConstructor((short) 325, SpatialFieldBinningStrategy::new),
        new PersistableIdAndConstructor((short) 326, OptimalVectorTimeRangeAggregation::new),
        new PersistableIdAndConstructor((short) 327, VectorBoundingBoxAggregation::new),
        new PersistableIdAndConstructor((short) 328, CommonIndexBoundingBoxAggregation::new),
        new PersistableIdAndConstructor((short) 329, OptimalVectorBoundingBoxAggregation::new),
        new PersistableIdAndConstructor((short) 330, OptimalCQLQuery::new),
        new PersistableIdAndConstructor((short) 331, SpatialQuery::new),
        new PersistableIdAndConstructor((short) 332, SpatialTemporalQuery::new),
        new PersistableIdAndConstructor((short) 333, TemporalQuery::new),
        new PersistableIdAndConstructor((short) 334, SimpleTimeDefinition::new),
        new PersistableIdAndConstructor((short) 335, SimpleTimeIndexStrategy::new),
        new PersistableIdAndConstructor((short) 336, CustomCRSBoundedSpatialDimensionX::new),
        new PersistableIdAndConstructor((short) 337, CustomCRSBoundedSpatialDimensionY::new),
        new PersistableIdAndConstructor((short) 338, SpatialSimpleFeatureBinningStrategy::new),
        new PersistableIdAndConstructor((short) 339, SpatialCommonIndexedBinningStrategy::new),
        new PersistableIdAndConstructor((short) 340, InternalGeotoolsDataAdapterWrapper::new),
        new PersistableIdAndConstructor((short) 341, SpatialFieldDescriptor::new),
        new PersistableIdAndConstructor((short) 342, LatitudeField::new),
        new PersistableIdAndConstructor((short) 343, LongitudeField::new),
        new PersistableIdAndConstructor((short) 344, CustomCRSSpatialField::new),
        new PersistableIdAndConstructor((short) 345, TemporalFieldDescriptor::new),
        new PersistableIdAndConstructor((short) 346, Crosses::new),
        new PersistableIdAndConstructor((short) 347, Disjoint::new),
        new PersistableIdAndConstructor((short) 348, Intersects::new),
        new PersistableIdAndConstructor((short) 349, Overlaps::new),
        // 350-358 are used by GeotimeRegisteredIndexFieldMappers
        new PersistableIdAndConstructor((short) 359, SpatialContains::new),
        new PersistableIdAndConstructor((short) 360, SpatialEqualTo::new),
        new PersistableIdAndConstructor((short) 361, SpatialNotEqualTo::new),
        new PersistableIdAndConstructor((short) 362, Touches::new),
        new PersistableIdAndConstructor((short) 363, Within::new),
        new PersistableIdAndConstructor((short) 364, PreparedFilterGeometry::new),
        new PersistableIdAndConstructor((short) 365, UnpreparedFilterGeometry::new),
        new PersistableIdAndConstructor((short) 366, SpatialFieldValue::new),
        new PersistableIdAndConstructor((short) 367, SpatialLiteral::new),
        new PersistableIdAndConstructor((short) 368, After::new),
        new PersistableIdAndConstructor((short) 369, Before::new),
        new PersistableIdAndConstructor((short) 370, BeforeOrDuring::new),
        new PersistableIdAndConstructor((short) 371, DuringOrAfter::new),
        new PersistableIdAndConstructor((short) 372, During::new),
        new PersistableIdAndConstructor((short) 373, TemporalBetween::new),
        new PersistableIdAndConstructor((short) 374, TimeOverlaps::new),
        new PersistableIdAndConstructor((short) 375, TemporalFieldValue::new),
        new PersistableIdAndConstructor((short) 376, TemporalLiteral::new),
        new PersistableIdAndConstructor((short) 377, BBox::new),
        new PersistableIdAndConstructor((short) 378, TemporalEqualTo::new),
        new PersistableIdAndConstructor((short) 379, TemporalNotEqualTo::new),
        new PersistableIdAndConstructor((short) 380, TextToSpatialExpression::new)};
  }
}
