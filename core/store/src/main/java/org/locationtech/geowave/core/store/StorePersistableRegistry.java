/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store;

import org.locationtech.geowave.core.index.persist.InternalPersistableRegistry;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;
import org.locationtech.geowave.core.store.adapter.BaseFieldDescriptor;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.adapter.BinaryDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterImpl;
import org.locationtech.geowave.core.store.adapter.SimpleRowTransform;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.data.visibility.FallbackVisibilityHandler;
import org.locationtech.geowave.core.store.data.visibility.FieldLevelVisibilityHandler;
import org.locationtech.geowave.core.store.data.visibility.FieldMappedVisibilityHandler;
import org.locationtech.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import org.locationtech.geowave.core.store.data.visibility.JsonFieldLevelVisibilityHandler;
import org.locationtech.geowave.core.store.data.visibility.UnconstrainedVisibilityHandler;
import org.locationtech.geowave.core.store.dimension.BasicNumericDimensionField;
import org.locationtech.geowave.core.store.index.AttributeIndexImpl;
import org.locationtech.geowave.core.store.index.BasicIndexModel;
import org.locationtech.geowave.core.store.index.CustomAttributeIndex;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.core.store.index.CustomNameIndex;
import org.locationtech.geowave.core.store.index.IndexImpl;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.index.TextAttributeIndexProvider.AdapterFieldTextIndexEntryConverter;
import org.locationtech.geowave.core.store.query.aggregate.BinningAggregation;
import org.locationtech.geowave.core.store.query.aggregate.BinningAggregationOptions;
import org.locationtech.geowave.core.store.query.aggregate.CountAggregation;
import org.locationtech.geowave.core.store.query.aggregate.FieldMinAggregation;
import org.locationtech.geowave.core.store.query.aggregate.FieldMaxAggregation;
import org.locationtech.geowave.core.store.query.aggregate.FieldSumAggregation;
import org.locationtech.geowave.core.store.query.aggregate.CompositeAggregation;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.query.aggregate.MergingAggregation;
import org.locationtech.geowave.core.store.query.aggregate.OptimalCountAggregation;
import org.locationtech.geowave.core.store.query.aggregate.OptimalCountAggregation.CommonIndexCountAggregation;
import org.locationtech.geowave.core.store.query.aggregate.OptimalCountAggregation.FieldCountAggregation;
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
import org.locationtech.geowave.core.store.query.constraints.ExplicitFilteredQuery;
import org.locationtech.geowave.core.store.query.constraints.FilteredEverythingQuery;
import org.locationtech.geowave.core.store.query.constraints.InsertionIdQuery;
import org.locationtech.geowave.core.store.query.constraints.OptimalExpressionQuery;
import org.locationtech.geowave.core.store.query.constraints.PrefixIdQuery;
import org.locationtech.geowave.core.store.query.constraints.SimpleNumericQuery;
import org.locationtech.geowave.core.store.query.filter.AdapterIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter;
import org.locationtech.geowave.core.store.query.filter.CoordinateRangeQueryFilter;
import org.locationtech.geowave.core.store.query.filter.DataIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.DataIdRangeQueryFilter;
import org.locationtech.geowave.core.store.query.filter.DedupeFilter;
import org.locationtech.geowave.core.store.query.filter.ExpressionQueryFilter;
import org.locationtech.geowave.core.store.query.filter.FilterList;
import org.locationtech.geowave.core.store.query.filter.InsertionIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.PrefixIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.expression.And;
import org.locationtech.geowave.core.store.query.filter.expression.BooleanFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.BooleanLiteral;
import org.locationtech.geowave.core.store.query.filter.expression.Exclude;
import org.locationtech.geowave.core.store.query.filter.expression.GenericEqualTo;
import org.locationtech.geowave.core.store.query.filter.expression.GenericFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.GenericLiteral;
import org.locationtech.geowave.core.store.query.filter.expression.GenericNotEqualTo;
import org.locationtech.geowave.core.store.query.filter.expression.Include;
import org.locationtech.geowave.core.store.query.filter.expression.IsNotNull;
import org.locationtech.geowave.core.store.query.filter.expression.IsNull;
import org.locationtech.geowave.core.store.query.filter.expression.Not;
import org.locationtech.geowave.core.store.query.filter.expression.Or;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.Abs;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.Add;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.Divide;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.Multiply;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericBetween;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericComparisonOperator;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericLiteral;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.Subtract;
import org.locationtech.geowave.core.store.query.filter.expression.text.Concat;
import org.locationtech.geowave.core.store.query.filter.expression.text.Contains;
import org.locationtech.geowave.core.store.query.filter.expression.text.EndsWith;
import org.locationtech.geowave.core.store.query.filter.expression.text.StartsWith;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextBetween;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextComparisonOperator;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextLiteral;
import org.locationtech.geowave.core.store.query.options.AggregateTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions.HintKey;
import org.locationtech.geowave.core.store.query.options.FilterByTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.QueryAllIndices;
import org.locationtech.geowave.core.store.query.options.QueryAllTypes;
import org.locationtech.geowave.core.store.query.options.QuerySingleIndex;

public class StorePersistableRegistry implements
    PersistableRegistrySpi,
    InternalPersistableRegistry {

  @Override
  public PersistableIdAndConstructor[] getSupportedPersistables() {
    return new PersistableIdAndConstructor[] {
        // 200 is a legacy class (pre 2.0)
        new PersistableIdAndConstructor((short) 201, BaseFieldDescriptor::new),
        // 202 is used by CoreRegisteredIndexFieldMappers
        new PersistableIdAndConstructor((short) 203, GlobalVisibilityHandler::new),
        new PersistableIdAndConstructor((short) 204, UnconstrainedVisibilityHandler::new),
        new PersistableIdAndConstructor((short) 205, FallbackVisibilityHandler::new),
        new PersistableIdAndConstructor((short) 206, FieldMappedVisibilityHandler::new),
        new PersistableIdAndConstructor((short) 207, FieldLevelVisibilityHandler::new),
        new PersistableIdAndConstructor((short) 208, AdapterIdQueryFilter::new),
        new PersistableIdAndConstructor((short) 209, BasicQueryFilter::new),
        new PersistableIdAndConstructor((short) 210, DataIdQueryFilter::new),
        new PersistableIdAndConstructor((short) 211, DedupeFilter::new),
        new PersistableIdAndConstructor((short) 212, DataIdQuery::new),
        new PersistableIdAndConstructor((short) 213, PrefixIdQueryFilter::new),
        new PersistableIdAndConstructor((short) 215, BasicIndexModel::new),
        new PersistableIdAndConstructor((short) 216, JsonFieldLevelVisibilityHandler::new),
        new PersistableIdAndConstructor((short) 217, IndexImpl::new),
        new PersistableIdAndConstructor((short) 218, CustomNameIndex::new),
        new PersistableIdAndConstructor((short) 219, NullIndex::new),
        new PersistableIdAndConstructor((short) 220, DataIdRangeQuery::new),
        new PersistableIdAndConstructor((short) 221, AttributeIndexImpl::new),
        new PersistableIdAndConstructor((short) 222, CustomAttributeIndex::new),
        new PersistableIdAndConstructor((short) 223, AdapterFieldTextIndexEntryConverter::new),
        new PersistableIdAndConstructor((short) 224, BooleanFieldValue::new),
        new PersistableIdAndConstructor((short) 225, BooleanLiteral::new),
        new PersistableIdAndConstructor((short) 226, GenericFieldValue::new),
        new PersistableIdAndConstructor((short) 227, GenericLiteral::new),
        new PersistableIdAndConstructor((short) 228, BasicQueryByClass::new),
        new PersistableIdAndConstructor((short) 229, CoordinateRangeQuery::new),
        new PersistableIdAndConstructor((short) 230, CoordinateRangeQueryFilter::new),
        new PersistableIdAndConstructor((short) 231, CommonQueryOptions::new),
        new PersistableIdAndConstructor((short) 232, DataIdRangeQueryFilter::new),
        new PersistableIdAndConstructor((short) 233, CountAggregation::new),
        new PersistableIdAndConstructor((short) 234, Include::new),
        new PersistableIdAndConstructor((short) 235, InsertionIdQueryFilter::new),
        new PersistableIdAndConstructor((short) 236, Exclude::new),
        new PersistableIdAndConstructor((short) 237, FilterByTypeQueryOptions::new),
        new PersistableIdAndConstructor((short) 238, QueryAllIndices::new),
        new PersistableIdAndConstructor((short) 239, And::new),
        new PersistableIdAndConstructor((short) 240, Or::new),
        new PersistableIdAndConstructor((short) 241, AggregateTypeQueryOptions::new),
        new PersistableIdAndConstructor((short) 242, AdapterMapping::new),
        new PersistableIdAndConstructor((short) 243, Not::new),
        new PersistableIdAndConstructor((short) 244, Query::new),
        new PersistableIdAndConstructor((short) 245, AggregationQuery::new),
        new PersistableIdAndConstructor((short) 246, NumericComparisonOperator::new),
        new PersistableIdAndConstructor((short) 247, TextComparisonOperator::new),
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
        // 262 is a legacy class (pre 2.0)
        new PersistableIdAndConstructor((short) 263, CustomIndex::new),
        new PersistableIdAndConstructor((short) 264, CustomQueryConstraints::new),
        new PersistableIdAndConstructor((short) 265, InternalCustomConstraints::new),
        new PersistableIdAndConstructor((short) 266, BinningAggregationOptions::new),
        new PersistableIdAndConstructor((short) 267, BinningAggregation::new),
        new PersistableIdAndConstructor((short) 268, CustomQueryConstraintsWithFilter::new),
        new PersistableIdAndConstructor((short) 269, InternalCustomQueryFilter::new),
        new PersistableIdAndConstructor((short) 270, InternalDataAdapterImpl::new),
        new PersistableIdAndConstructor((short) 271, BasicNumericDimensionField::new),
        new PersistableIdAndConstructor((short) 272, DataStoreProperty::new),
        new PersistableIdAndConstructor((short) 273, AdapterToIndexMapping::new),
        new PersistableIdAndConstructor((short) 274, HintKey::new),
        new PersistableIdAndConstructor((short) 275, NumericBetween::new),
        new PersistableIdAndConstructor((short) 276, Abs::new),
        new PersistableIdAndConstructor((short) 277, Add::new),
        new PersistableIdAndConstructor((short) 278, Subtract::new),
        new PersistableIdAndConstructor((short) 279, Multiply::new),
        new PersistableIdAndConstructor((short) 280, Divide::new),
        new PersistableIdAndConstructor((short) 281, NumericFieldValue::new),
        new PersistableIdAndConstructor((short) 282, NumericLiteral::new),
        new PersistableIdAndConstructor((short) 283, Concat::new),
        new PersistableIdAndConstructor((short) 284, Contains::new),
        new PersistableIdAndConstructor((short) 285, EndsWith::new),
        new PersistableIdAndConstructor((short) 286, StartsWith::new),
        new PersistableIdAndConstructor((short) 287, TextFieldValue::new),
        new PersistableIdAndConstructor((short) 288, TextLiteral::new),
        new PersistableIdAndConstructor((short) 289, TextBetween::new),
        new PersistableIdAndConstructor((short) 290, IsNotNull::new),
        new PersistableIdAndConstructor((short) 291, OptimalExpressionQuery::new),
        new PersistableIdAndConstructor((short) 292, GenericNotEqualTo::new),
        new PersistableIdAndConstructor((short) 293, GenericEqualTo::new),
        new PersistableIdAndConstructor((short) 294, ExplicitFilteredQuery::new),
        new PersistableIdAndConstructor((short) 295, ExpressionQueryFilter::new),
        new PersistableIdAndConstructor((short) 296, FilteredEverythingQuery::new),
        new PersistableIdAndConstructor((short) 297, BasicDataTypeAdapter::new),
        new PersistableIdAndConstructor((short) 298, IsNull::new),
        new PersistableIdAndConstructor((short) 299, FieldNameParam::new),
        // use 3000+ range
        new PersistableIdAndConstructor((short) 3000, OptimalCountAggregation::new),
        new PersistableIdAndConstructor((short) 3001, CommonIndexCountAggregation::new),
        new PersistableIdAndConstructor((short) 3002, FieldCountAggregation::new),
        new PersistableIdAndConstructor((short) 3003, FieldMaxAggregation::new),
        new PersistableIdAndConstructor((short) 3004, FieldMinAggregation::new),
        new PersistableIdAndConstructor((short) 3005, FieldSumAggregation::new),
        new PersistableIdAndConstructor((short) 3006, CompositeAggregation::new)};
  }
}
