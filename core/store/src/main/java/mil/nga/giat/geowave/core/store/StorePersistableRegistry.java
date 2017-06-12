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
package mil.nga.giat.geowave.core.store;

import mil.nga.giat.geowave.core.index.persist.PersistableRegistrySpi;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.adapter.statistics.MaxDuplicatesStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.data.visibility.FieldVisibilityCount;
import mil.nga.giat.geowave.core.store.filter.AdapterIdQueryFilter;
import mil.nga.giat.geowave.core.store.filter.BasicQueryFilter;
import mil.nga.giat.geowave.core.store.filter.DataIdQueryFilter;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.PrefixIdQueryFilter;
import mil.nga.giat.geowave.core.store.filter.RowIdQueryFilter;
import mil.nga.giat.geowave.core.store.index.BasicIndexModel;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.core.store.index.NullIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.numeric.NumberRangeFilter;
import mil.nga.giat.geowave.core.store.index.numeric.NumericFieldIndexStrategy;
import mil.nga.giat.geowave.core.store.index.numeric.NumericIndexStrategy;
import mil.nga.giat.geowave.core.store.index.temporal.DateRangeFilter;
import mil.nga.giat.geowave.core.store.index.temporal.TemporalIndexStrategy;
import mil.nga.giat.geowave.core.store.index.text.TextExactMatchFilter;
import mil.nga.giat.geowave.core.store.index.text.TextIndexStrategy;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.CoordinateRangeQuery;
import mil.nga.giat.geowave.core.store.query.CoordinateRangeQueryFilter;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.CountAggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CountResult;
import mil.nga.giat.geowave.core.store.query.aggregate.DataStatisticsAggregation;

public class StorePersistableRegistry implements
		PersistableRegistrySpi
{

	@Override
	public PersistableIdAndConstructor[] getSupportedPersistables() {
		return new PersistableIdAndConstructor[] {
			new PersistableIdAndConstructor(
					(short) 200,
					AdapterToIndexMapping::new),
			new PersistableIdAndConstructor(
					(short) 201,
					CountDataStatistics::new),
			new PersistableIdAndConstructor(
					(short) 202,
					DuplicateEntryCount::new),
			new PersistableIdAndConstructor(
					(short) 203,
					MaxDuplicatesStatistics::new),
			new PersistableIdAndConstructor(
					(short) 204,
					RowRangeDataStatistics::new),
			new PersistableIdAndConstructor(
					(short) 205,
					RowRangeHistogramStatistics::new),
			new PersistableIdAndConstructor(
					(short) 206,
					DifferingFieldVisibilityEntryCount::new),
			new PersistableIdAndConstructor(
					(short) 207,
					FieldVisibilityCount::new),
			new PersistableIdAndConstructor(
					(short) 208,
					AdapterIdQueryFilter::new),
			new PersistableIdAndConstructor(
					(short) 209,
					BasicQueryFilter::new),
			new PersistableIdAndConstructor(
					(short) 210,
					DataIdQueryFilter::new),
			new PersistableIdAndConstructor(
					(short) 211,
					DedupeFilter::new),
			new PersistableIdAndConstructor(
					(short) 212,
					DistributableFilterList::new),
			new PersistableIdAndConstructor(
					(short) 213,
					PrefixIdQueryFilter::new),
			new PersistableIdAndConstructor(
					(short) 214,
					RowIdQueryFilter::new),
			new PersistableIdAndConstructor(
					(short) 215,
					BasicIndexModel::new),
			new PersistableIdAndConstructor(
					(short) 216,
					IndexMetaDataSet::new),
			new PersistableIdAndConstructor(
					(short) 217,
					PrimaryIndex::new),
			new PersistableIdAndConstructor(
					(short) 218,
					CustomIdIndex::new),
			new PersistableIdAndConstructor(
					(short) 219,
					NullIndex::new),
			new PersistableIdAndConstructor(
					(short) 220,
					SecondaryIndex::new),
			new PersistableIdAndConstructor(
					(short) 221,
					NumberRangeFilter::new),
			new PersistableIdAndConstructor(
					(short) 222,
					NumericFieldIndexStrategy::new),
			new PersistableIdAndConstructor(
					(short) 223,
					NumericIndexStrategy::new),
			new PersistableIdAndConstructor(
					(short) 224,
					DateRangeFilter::new),
			new PersistableIdAndConstructor(
					(short) 225,
					TemporalIndexStrategy::new),
			new PersistableIdAndConstructor(
					(short) 226,
					TextExactMatchFilter::new),
			new PersistableIdAndConstructor(
					(short) 227,
					TextIndexStrategy::new),
			new PersistableIdAndConstructor(
					(short) 228,
					BasicQuery::new),
			new PersistableIdAndConstructor(
					(short) 229,
					CoordinateRangeQuery::new),
			new PersistableIdAndConstructor(
					(short) 230,
					CoordinateRangeQueryFilter::new),
			new PersistableIdAndConstructor(
					(short) 231,
					CountResult::new),
			new PersistableIdAndConstructor(
					(short) 232,
					QueryOptions::new),
			new PersistableIdAndConstructor(
					(short) 233,
					CountAggregation::new),
			new PersistableIdAndConstructor(
					(short) 234,
					DataStatisticsAggregation::new)
		};
	}
}
