/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics;

import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.DataTypeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.FieldValueBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.NumericRangeFieldValueBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.PartitionBinningStrategy;
import org.locationtech.geowave.core.store.statistics.field.BloomFilterStatistic;
import org.locationtech.geowave.core.store.statistics.field.BloomFilterStatistic.BloomFilterValue;
import org.locationtech.geowave.core.store.statistics.field.CountMinSketchStatistic;
import org.locationtech.geowave.core.store.statistics.field.CountMinSketchStatistic.CountMinSketchValue;
import org.locationtech.geowave.core.store.statistics.field.FixedBinNumericHistogramStatistic;
import org.locationtech.geowave.core.store.statistics.field.FixedBinNumericHistogramStatistic.FixedBinNumericHistogramValue;
import org.locationtech.geowave.core.store.statistics.field.HyperLogLogStatistic;
import org.locationtech.geowave.core.store.statistics.field.HyperLogLogStatistic.HyperLogLogPlusValue;
import org.locationtech.geowave.core.store.statistics.field.NumericHistogramStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericHistogramStatistic.NumericHistogramValue;
import org.locationtech.geowave.core.store.statistics.field.NumericMeanStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericMeanStatistic.NumericMeanValue;
import org.locationtech.geowave.core.store.statistics.field.NumericRangeStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericRangeStatistic.NumericRangeValue;
import org.locationtech.geowave.core.store.statistics.field.NumericStatsStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericStatsStatistic.NumericStatsValue;
import org.locationtech.geowave.core.store.statistics.index.DifferingVisibilityCountStatistic;
import org.locationtech.geowave.core.store.statistics.index.DifferingVisibilityCountStatistic.DifferingVisibilityCountValue;
import org.locationtech.geowave.core.store.statistics.index.DuplicateEntryCountStatistic;
import org.locationtech.geowave.core.store.statistics.index.DuplicateEntryCountStatistic.DuplicateEntryCountValue;
import org.locationtech.geowave.core.store.statistics.index.FieldVisibilityCountStatistic;
import org.locationtech.geowave.core.store.statistics.index.FieldVisibilityCountStatistic.FieldVisibilityCountValue;
import org.locationtech.geowave.core.store.statistics.index.IndexMetaDataSetStatistic;
import org.locationtech.geowave.core.store.statistics.index.IndexMetaDataSetStatistic.IndexMetaDataSetValue;
import org.locationtech.geowave.core.store.statistics.index.MaxDuplicatesStatistic;
import org.locationtech.geowave.core.store.statistics.index.MaxDuplicatesStatistic.MaxDuplicatesValue;
import org.locationtech.geowave.core.store.statistics.index.PartitionsStatistic;
import org.locationtech.geowave.core.store.statistics.index.PartitionsStatistic.PartitionsValue;
import org.locationtech.geowave.core.store.statistics.index.RowRangeHistogramStatistic;
import org.locationtech.geowave.core.store.statistics.index.RowRangeHistogramStatistic.RowRangeHistogramValue;

public class CoreRegisteredStatistics implements StatisticsRegistrySPI {

  @Override
  public RegisteredStatistic[] getRegisteredStatistics() {
    return new RegisteredStatistic[] {
        // Index Statistics
        new RegisteredStatistic(
            DifferingVisibilityCountStatistic.STATS_TYPE,
            DifferingVisibilityCountStatistic::new,
            DifferingVisibilityCountValue::new,
            (short) 2000,
            (short) 2001),
        new RegisteredStatistic(
            DuplicateEntryCountStatistic.STATS_TYPE,
            DuplicateEntryCountStatistic::new,
            DuplicateEntryCountValue::new,
            (short) 2002,
            (short) 2003),
        new RegisteredStatistic(
            FieldVisibilityCountStatistic.STATS_TYPE,
            FieldVisibilityCountStatistic::new,
            FieldVisibilityCountValue::new,
            (short) 2004,
            (short) 2005),
        new RegisteredStatistic(
            IndexMetaDataSetStatistic.STATS_TYPE,
            IndexMetaDataSetStatistic::new,
            IndexMetaDataSetValue::new,
            (short) 2006,
            (short) 2007),
        new RegisteredStatistic(
            MaxDuplicatesStatistic.STATS_TYPE,
            MaxDuplicatesStatistic::new,
            MaxDuplicatesValue::new,
            (short) 2008,
            (short) 2009),
        new RegisteredStatistic(
            PartitionsStatistic.STATS_TYPE,
            PartitionsStatistic::new,
            PartitionsValue::new,
            (short) 2010,
            (short) 2011),
        new RegisteredStatistic(
            RowRangeHistogramStatistic.STATS_TYPE,
            RowRangeHistogramStatistic::new,
            RowRangeHistogramValue::new,
            (short) 2012,
            (short) 2013),

        // Data Type Statistics
        new RegisteredStatistic(
            CountStatistic.STATS_TYPE,
            CountStatistic::new,
            CountValue::new,
            (short) 2014,
            (short) 2015),

        // Field Statistics
        new RegisteredStatistic(
            FixedBinNumericHistogramStatistic.STATS_TYPE,
            FixedBinNumericHistogramStatistic::new,
            FixedBinNumericHistogramValue::new,
            (short) 2016,
            (short) 2017),
        new RegisteredStatistic(
            NumericRangeStatistic.STATS_TYPE,
            NumericRangeStatistic::new,
            NumericRangeValue::new,
            (short) 2018,
            (short) 2019),
        new RegisteredStatistic(
            CountMinSketchStatistic.STATS_TYPE,
            CountMinSketchStatistic::new,
            CountMinSketchValue::new,
            (short) 2020,
            (short) 2021),
        new RegisteredStatistic(
            HyperLogLogStatistic.STATS_TYPE,
            HyperLogLogStatistic::new,
            HyperLogLogPlusValue::new,
            (short) 2022,
            (short) 2023),
        new RegisteredStatistic(
            NumericMeanStatistic.STATS_TYPE,
            NumericMeanStatistic::new,
            NumericMeanValue::new,
            (short) 2026,
            (short) 2027),
        new RegisteredStatistic(
            NumericStatsStatistic.STATS_TYPE,
            NumericStatsStatistic::new,
            NumericStatsValue::new,
            (short) 2028,
            (short) 2029),
        new RegisteredStatistic(
            NumericHistogramStatistic.STATS_TYPE,
            NumericHistogramStatistic::new,
            NumericHistogramValue::new,
            (short) 2030,
            (short) 2031),
        new RegisteredStatistic(
            BloomFilterStatistic.STATS_TYPE,
            BloomFilterStatistic::new,
            BloomFilterValue::new,
            (short) 2032,
            (short) 2033),};
  }

  @Override
  public RegisteredBinningStrategy[] getRegisteredBinningStrategies() {
    return new RegisteredBinningStrategy[] {
        new RegisteredBinningStrategy(
            PartitionBinningStrategy.NAME,
            PartitionBinningStrategy::new,
            (short) 2050),
        new RegisteredBinningStrategy(
            DataTypeBinningStrategy.NAME,
            DataTypeBinningStrategy::new,
            (short) 2051),
        new RegisteredBinningStrategy(
            CompositeBinningStrategy.NAME,
            CompositeBinningStrategy::new,
            (short) 2052),
        new RegisteredBinningStrategy(
            FieldValueBinningStrategy.NAME,
            FieldValueBinningStrategy::new,
            (short) 2053),
        new RegisteredBinningStrategy(
            NumericRangeFieldValueBinningStrategy.NAME,
            NumericRangeFieldValueBinningStrategy::new,
            (short) 2054)};
  }
}
