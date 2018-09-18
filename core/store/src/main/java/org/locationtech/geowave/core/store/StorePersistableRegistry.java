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
package org.locationtech.geowave.core.store;

import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;
import org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import org.locationtech.geowave.core.store.adapter.statistics.MaxDuplicatesStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.PartitionStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryOptions;
import org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import org.locationtech.geowave.core.store.data.visibility.FieldVisibilityCount;
import org.locationtech.geowave.core.store.index.BasicIndexModel;
import org.locationtech.geowave.core.store.index.CustomIdIndex;
import org.locationtech.geowave.core.store.index.IndexMetaDataSet;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.index.PrimaryIndex;
import org.locationtech.geowave.core.store.index.SecondaryIndexImpl;
import org.locationtech.geowave.core.store.index.numeric.NumberRangeFilter;
import org.locationtech.geowave.core.store.index.numeric.NumericFieldIndexStrategy;
import org.locationtech.geowave.core.store.index.temporal.DateRangeFilter;
import org.locationtech.geowave.core.store.index.temporal.TemporalIndexStrategy;
import org.locationtech.geowave.core.store.index.text.TextExactMatchFilter;
import org.locationtech.geowave.core.store.index.text.TextIndexStrategy;
import org.locationtech.geowave.core.store.query.BasicQuery;
import org.locationtech.geowave.core.store.query.CoordinateRangeQuery;
import org.locationtech.geowave.core.store.query.aggregate.CountAggregation;
import org.locationtech.geowave.core.store.query.aggregate.CountResult;
import org.locationtech.geowave.core.store.query.aggregate.DataStatisticsAggregation;
import org.locationtech.geowave.core.store.query.filter.AdapterIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter;
import org.locationtech.geowave.core.store.query.filter.CoordinateRangeQueryFilter;
import org.locationtech.geowave.core.store.query.filter.DataIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.DedupeFilter;
import org.locationtech.geowave.core.store.query.filter.DistributableFilterList;
import org.locationtech.geowave.core.store.query.filter.InsertionIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.PrefixIdQueryFilter;

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
					SecondaryIndexImpl::new),
			new PersistableIdAndConstructor(
					(short) 221,
					NumberRangeFilter::new),
			new PersistableIdAndConstructor(
					(short) 222,
					NumericFieldIndexStrategy::new),
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
					DataStatisticsAggregation::new),
			new PersistableIdAndConstructor(
					(short) 235,
					InsertionIdQueryFilter::new),
			new PersistableIdAndConstructor(
					(short) 236,
					PartitionStatistics::new)
		};
	}
}
