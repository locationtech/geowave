/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store;

import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;
import org.locationtech.geowave.core.store.adapter.BaseFieldDescriptor;
import org.locationtech.geowave.core.store.adapter.BinaryDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterImpl;
import org.locationtech.geowave.core.store.adapter.SimpleRowTransform;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.dimension.BasicNumericDimensionField;
import org.locationtech.geowave.core.store.index.BasicIndexModel;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.core.store.index.CustomNameIndex;
import org.locationtech.geowave.core.store.index.IndexImpl;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.query.aggregate.BinningAggregation;
import org.locationtech.geowave.core.store.query.aggregate.BinningAggregationOptions;
import org.locationtech.geowave.core.store.query.aggregate.CountAggregation;
import org.locationtech.geowave.core.store.query.aggregate.MergingAggregation;
import org.locationtech.geowave.core.store.query.constraints.BasicOrderedConstraintQuery;
import org.locationtech.geowave.core.store.query.constraints.BasicOrderedConstraintQuery.OrderedConstraints;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass.ConstraintsByClass;
import org.locationtech.geowave.core.store.query.constraints.CoordinateRangeQuery;
import org.locationtech.geowave.core.store.query.constraints.CustomQueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.CustomQueryConstraints.InternalCustomConstraints;
import org.locationtech.geowave.core.store.query.constraints.CustomQueryConstraintsWithFilter;
import org.locationtech.geowave.core.store.query.constraints.CustomQueryConstraintsWithFilter.InternalCustomQueryFilter;
import org.locationtech.geowave.core.store.query.constraints.DataIdQuery;
import org.locationtech.geowave.core.store.query.constraints.DataIdRangeQuery;
import org.locationtech.geowave.core.store.query.constraints.EverythingQuery;
import org.locationtech.geowave.core.store.query.constraints.InsertionIdQuery;
import org.locationtech.geowave.core.store.query.constraints.PrefixIdQuery;
import org.locationtech.geowave.core.store.query.constraints.SimpleNumericQuery;
import org.locationtech.geowave.core.store.query.filter.AdapterIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter;
import org.locationtech.geowave.core.store.query.filter.CoordinateRangeQueryFilter;
import org.locationtech.geowave.core.store.query.filter.DataIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.DataIdRangeQueryFilter;
import org.locationtech.geowave.core.store.query.filter.DedupeFilter;
import org.locationtech.geowave.core.store.query.filter.FilterList;
import org.locationtech.geowave.core.store.query.filter.InsertionIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.PrefixIdQueryFilter;
import org.locationtech.geowave.core.store.query.options.AggregateTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.FilterByTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.QueryAllIndices;
import org.locationtech.geowave.core.store.query.options.QueryAllTypes;
import org.locationtech.geowave.core.store.query.options.QuerySingleIndex;

public class StorePersistableRegistry implements PersistableRegistrySpi {

  @Override
  public PersistableIdAndConstructor[] getSupportedPersistables() {
    return new PersistableIdAndConstructor[] {
        new PersistableIdAndConstructor((short) 200, AdapterToIndexMapping::new),
        new PersistableIdAndConstructor((short) 201, BaseFieldDescriptor::new),
        // 202 is used by CoreRegisteredIndexFieldMappers
        // 203-207 Unused
        new PersistableIdAndConstructor((short) 208, AdapterIdQueryFilter::new),
        new PersistableIdAndConstructor((short) 209, BasicQueryFilter::new),
        new PersistableIdAndConstructor((short) 210, DataIdQueryFilter::new),
        new PersistableIdAndConstructor((short) 211, DedupeFilter::new),
        new PersistableIdAndConstructor((short) 212, DataIdQuery::new),
        new PersistableIdAndConstructor((short) 213, PrefixIdQueryFilter::new),
        new PersistableIdAndConstructor((short) 215, BasicIndexModel::new),
        // 216 Unused
        new PersistableIdAndConstructor((short) 217, IndexImpl::new),
        new PersistableIdAndConstructor((short) 218, CustomNameIndex::new),
        new PersistableIdAndConstructor((short) 219, NullIndex::new),
        new PersistableIdAndConstructor((short) 220, DataIdRangeQuery::new),
        // 221-227 Unused
        new PersistableIdAndConstructor((short) 228, BasicQueryByClass::new),
        new PersistableIdAndConstructor((short) 229, CoordinateRangeQuery::new),
        new PersistableIdAndConstructor((short) 230, CoordinateRangeQueryFilter::new),
        new PersistableIdAndConstructor((short) 231, CommonQueryOptions::new),
        new PersistableIdAndConstructor((short) 232, DataIdRangeQueryFilter::new),
        new PersistableIdAndConstructor((short) 233, CountAggregation::new),
        // 234 Unused
        new PersistableIdAndConstructor((short) 235, InsertionIdQueryFilter::new),
        // 236 Unused
        new PersistableIdAndConstructor((short) 237, FilterByTypeQueryOptions::new),
        new PersistableIdAndConstructor((short) 238, QueryAllIndices::new),
        // 239-240 Unused
        new PersistableIdAndConstructor((short) 241, AggregateTypeQueryOptions::new),
        new PersistableIdAndConstructor((short) 242, AdapterMapping::new),
        // 243 Unused
        new PersistableIdAndConstructor((short) 244, Query::new),
        new PersistableIdAndConstructor((short) 245, AggregationQuery::new),
        // 246-247 Unused
        new PersistableIdAndConstructor((short) 248, QuerySingleIndex::new),
        new PersistableIdAndConstructor((short) 249, QueryAllTypes::new),
        new PersistableIdAndConstructor((short) 250, FilterList::new),
        new PersistableIdAndConstructor((short) 251, PrefixIdQuery::new),
        new PersistableIdAndConstructor((short) 252, InsertionIdQuery::new),
        new PersistableIdAndConstructor((short) 253, EverythingQuery::new),
        new PersistableIdAndConstructor((short) 254, SimpleRowTransform::new),
        new PersistableIdAndConstructor((short) 255, MergingAggregation::new),
        new PersistableIdAndConstructor((short) 256, SimpleNumericQuery::new),
        new PersistableIdAndConstructor((short) 257, ConstraintsByClass::new),
        new PersistableIdAndConstructor((short) 258, OrderedConstraints::new),
        new PersistableIdAndConstructor((short) 259, BasicOrderedConstraintQuery::new),
        new PersistableIdAndConstructor((short) 260, BasicQuery::new),
        new PersistableIdAndConstructor((short) 261, BinaryDataAdapter::new),
        // 262 is Unused
        new PersistableIdAndConstructor((short) 263, CustomIndex::new),
        new PersistableIdAndConstructor((short) 264, CustomQueryConstraints::new),
        new PersistableIdAndConstructor((short) 265, InternalCustomConstraints::new),
        new PersistableIdAndConstructor((short) 266, BinningAggregationOptions::new),
        new PersistableIdAndConstructor((short) 267, BinningAggregation::new),
        new PersistableIdAndConstructor((short) 268, CustomQueryConstraintsWithFilter::new),
        new PersistableIdAndConstructor((short) 269, InternalCustomQueryFilter::new),
        new PersistableIdAndConstructor((short) 270, InternalDataAdapterImpl::new),
        new PersistableIdAndConstructor((short) 271, BasicNumericDimensionField::new)};
  }
}
