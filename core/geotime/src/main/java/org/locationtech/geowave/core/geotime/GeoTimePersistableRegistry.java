/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime;

import org.locationtech.geowave.core.geotime.adapter.SpatialFieldDescriptor;
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
import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.geotime.store.query.aggregate.SpatialCommonIndexedBinningStrategy;
import org.locationtech.geowave.core.geotime.store.query.aggregate.SpatialSimpleFeatureBinningStrategy;
import org.locationtech.geowave.core.geotime.store.query.aggregate.OptimalVectorBoundingBoxAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.OptimalVectorTimeRangeAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.VectorBoundingBoxAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.VectorTimeRangeAggregation;
import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;

public class GeoTimePersistableRegistry implements PersistableRegistrySpi {

  @Override
  public PersistableIdAndConstructor[] getSupportedPersistables() {
    return new PersistableIdAndConstructor[] {
        new PersistableIdAndConstructor((short) 300, LatitudeDefinition::new),
        new PersistableIdAndConstructor((short) 301, LongitudeDefinition::new),
        new PersistableIdAndConstructor((short) 302, TemporalBinningStrategy::new),
        new PersistableIdAndConstructor((short) 303, TimeDefinition::new),
        new PersistableIdAndConstructor((short) 304, LatitudeField::new),
        new PersistableIdAndConstructor((short) 305, LongitudeField::new),
        // 306-309 are unused
        new PersistableIdAndConstructor((short) 310, TimeField::new),
        new PersistableIdAndConstructor((short) 311, SpatialQueryFilter::new),
        new PersistableIdAndConstructor((short) 312, ExplicitSpatialQuery::new),
        new PersistableIdAndConstructor((short) 313, CustomCRSSpatialField::new),
        new PersistableIdAndConstructor((short) 314, CustomCRSBoundedSpatialDimension::new),
        new PersistableIdAndConstructor((short) 315, CustomCrsIndexModel::new),
        new PersistableIdAndConstructor((short) 316, IndexOnlySpatialQuery::new),
        new PersistableIdAndConstructor((short) 317, ExplicitSpatialTemporalQuery::new),
        new PersistableIdAndConstructor((short) 318, ExplicitTemporalQuery::new),
        new PersistableIdAndConstructor((short) 319, CustomCRSUnboundedSpatialDimension::new),
        // 320 is unused
        new PersistableIdAndConstructor((short) 321, CustomCRSUnboundedSpatialDimensionX::new),
        new PersistableIdAndConstructor((short) 322, CustomCRSUnboundedSpatialDimensionY::new),
        new PersistableIdAndConstructor((short) 323, VectorTimeRangeAggregation::new),
        new PersistableIdAndConstructor((short) 324, CommonIndexTimeRangeAggregation::new),
        new PersistableIdAndConstructor((short) 325, FieldNameParam::new),
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
        new PersistableIdAndConstructor((short) 341, SpatialFieldDescriptor::new)};
  }
}
