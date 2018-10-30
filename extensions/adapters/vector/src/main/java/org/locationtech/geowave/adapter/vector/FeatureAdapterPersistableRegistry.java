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
package org.locationtech.geowave.adapter.vector;

import org.locationtech.geowave.adapter.vector.index.NumericSecondaryIndexConfiguration;
import org.locationtech.geowave.adapter.vector.index.SecondaryIndexManager;
import org.locationtech.geowave.adapter.vector.index.SimpleFeaturePrimaryIndexConfiguration;
import org.locationtech.geowave.adapter.vector.index.TemporalSecondaryIndexConfiguration;
import org.locationtech.geowave.adapter.vector.index.TextSecondaryIndexConfiguration;
import org.locationtech.geowave.adapter.vector.ingest.CQLFilterOptionProvider;
import org.locationtech.geowave.adapter.vector.ingest.DataSchemaOptionProvider;
import org.locationtech.geowave.adapter.vector.ingest.FeatureSerializationOptionProvider;
import org.locationtech.geowave.adapter.vector.ingest.GeometrySimpOptionProvider;
import org.locationtech.geowave.adapter.vector.ingest.TypeNameOptionProvider;
import org.locationtech.geowave.adapter.vector.plugin.visibility.VisibilityConfiguration;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderAggregation;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderOptions;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderResult;
import org.locationtech.geowave.adapter.vector.render.PersistableComposite;
import org.locationtech.geowave.adapter.vector.render.PersistableRenderedImage;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderResult.CompositeGroupResult;
import org.locationtech.geowave.adapter.vector.stats.FeatureCountMinSketchStatistics;
import org.locationtech.geowave.adapter.vector.stats.FeatureFixedBinNumericStatistics;
import org.locationtech.geowave.adapter.vector.stats.FeatureHyperLogLogStatistics;
import org.locationtech.geowave.adapter.vector.stats.FeatureNumericHistogramStatistics;
import org.locationtech.geowave.adapter.vector.stats.FeatureNumericRangeStatistics;
import org.locationtech.geowave.adapter.vector.stats.StatsConfigurationCollection;
import org.locationtech.geowave.adapter.vector.stats.FeatureCountMinSketchStatistics.FeatureCountMinSketchConfig;
import org.locationtech.geowave.adapter.vector.stats.FeatureFixedBinNumericStatistics.FeatureFixedBinConfig;
import org.locationtech.geowave.adapter.vector.stats.FeatureHyperLogLogStatistics.FeatureHyperLogLogConfig;
import org.locationtech.geowave.adapter.vector.stats.FeatureNumericHistogramStatistics.FeatureNumericHistogramConfig;
import org.locationtech.geowave.adapter.vector.stats.FeatureNumericRangeStatistics.FeatureNumericRangeConfig;
import org.locationtech.geowave.adapter.vector.stats.StatsConfigurationCollection.SimpleFeatureStatsConfigurationCollection;
import org.locationtech.geowave.adapter.vector.util.SimpleFeatureUserDataConfigurationSet;
import org.locationtech.geowave.core.geotime.store.query.ExplicitCQLQuery;
import org.locationtech.geowave.core.geotime.store.query.filter.CQLQueryFilter;
import org.locationtech.geowave.core.geotime.store.statistics.FeatureBoundingBoxStatistics;
import org.locationtech.geowave.core.geotime.store.statistics.FeatureTimeRangeStatistics;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors.TimeDescriptorConfiguration;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;

public class FeatureAdapterPersistableRegistry implements
		PersistableRegistrySpi
{

	@Override
	public PersistableIdAndConstructor[] getSupportedPersistables() {
		return new PersistableIdAndConstructor[] {
			new PersistableIdAndConstructor(
					(short) 500,
					AvroFeatureDataAdapter::new),
			new PersistableIdAndConstructor(
					(short) 501,
					FeatureDataAdapter::new),
			new PersistableIdAndConstructor(
					(short) 502,
					SecondaryIndexManager::new),
			new PersistableIdAndConstructor(
					(short) 503,
					CQLFilterOptionProvider::new),
			new PersistableIdAndConstructor(
					(short) 504,
					DataSchemaOptionProvider::new),
			new PersistableIdAndConstructor(
					(short) 505,
					FeatureSerializationOptionProvider::new),
			new PersistableIdAndConstructor(
					(short) 506,
					TypeNameOptionProvider::new),
			new PersistableIdAndConstructor(
					(short) 507,
					ExplicitCQLQuery::new),
			new PersistableIdAndConstructor(
					(short) 508,
					CQLQueryFilter::new),
			new PersistableIdAndConstructor(
					(short) 509,
					DistributedRenderOptions::new),
			new PersistableIdAndConstructor(
					(short) 510,
					CompositeGroupResult::new),
			new PersistableIdAndConstructor(
					(short) 511,
					DistributedRenderResult::new),
			new PersistableIdAndConstructor(
					(short) 512,
					PersistableComposite::new),
			new PersistableIdAndConstructor(
					(short) 513,
					PersistableRenderedImage::new),
			new PersistableIdAndConstructor(
					(short) 514,
					FeatureBoundingBoxStatistics::new),
			new PersistableIdAndConstructor(
					(short) 515,
					FeatureCountMinSketchStatistics::new),
			new PersistableIdAndConstructor(
					(short) 516,
					FeatureFixedBinNumericStatistics::new),
			new PersistableIdAndConstructor(
					(short) 517,
					FeatureHyperLogLogStatistics::new),
			new PersistableIdAndConstructor(
					(short) 518,
					FeatureNumericHistogramStatistics::new),
			new PersistableIdAndConstructor(
					(short) 519,
					FeatureNumericRangeStatistics::new),
			new PersistableIdAndConstructor(
					(short) 520,
					FeatureTimeRangeStatistics::new),
			new PersistableIdAndConstructor(
					(short) 521,
					DistributedRenderAggregation::new),
			new PersistableIdAndConstructor(
					(short) 522,
					SimpleFeatureUserDataConfigurationSet::new),
			new PersistableIdAndConstructor(
					(short) 523,
					TimeDescriptorConfiguration::new),
			new PersistableIdAndConstructor(
					(short) 524,
					VisibilityConfiguration::new),
			new PersistableIdAndConstructor(
					(short) 525,
					SimpleFeatureStatsConfigurationCollection::new),
			new PersistableIdAndConstructor(
					(short) 526,
					StatsConfigurationCollection::new),
			new PersistableIdAndConstructor(
					(short) 527,
					FeatureCountMinSketchConfig::new),
			new PersistableIdAndConstructor(
					(short) 528,
					FeatureFixedBinConfig::new),
			new PersistableIdAndConstructor(
					(short) 529,
					FeatureHyperLogLogConfig::new),
			new PersistableIdAndConstructor(
					(short) 530,
					FeatureNumericHistogramConfig::new),
			new PersistableIdAndConstructor(
					(short) 531,
					FeatureNumericRangeConfig::new),
			new PersistableIdAndConstructor(
					(short) 532,
					NumericSecondaryIndexConfiguration::new),
			new PersistableIdAndConstructor(
					(short) 533,
					SimpleFeaturePrimaryIndexConfiguration::new),
			new PersistableIdAndConstructor(
					(short) 534,
					TemporalSecondaryIndexConfiguration::new),
			new PersistableIdAndConstructor(
					(short) 535,
					TextSecondaryIndexConfiguration::new),
			new PersistableIdAndConstructor(
					(short) 536,
					GeometrySimpOptionProvider::new)			
		};
	}
}
