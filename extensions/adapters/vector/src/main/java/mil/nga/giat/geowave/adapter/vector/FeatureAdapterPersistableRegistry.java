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
package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.adapter.vector.index.NumericSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.index.SecondaryIndexManager;
import mil.nga.giat.geowave.adapter.vector.index.SimpleFeaturePrimaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.index.TemporalSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.index.TextSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.ingest.CQLFilterOptionProvider;
import mil.nga.giat.geowave.adapter.vector.ingest.DataSchemaOptionProvider;
import mil.nga.giat.geowave.adapter.vector.ingest.FeatureSerializationOptionProvider;
import mil.nga.giat.geowave.adapter.vector.ingest.TypeNameOptionProvider;
import mil.nga.giat.geowave.adapter.vector.plugin.visibility.VisibilityConfiguration;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQueryFilter;
import mil.nga.giat.geowave.adapter.vector.render.DistributedRenderAggregation;
import mil.nga.giat.geowave.adapter.vector.render.DistributedRenderOptions;
import mil.nga.giat.geowave.adapter.vector.render.DistributedRenderResult;
import mil.nga.giat.geowave.adapter.vector.render.DistributedRenderResult.CompositeGroupResult;
import mil.nga.giat.geowave.adapter.vector.render.PersistableComposite;
import mil.nga.giat.geowave.adapter.vector.render.PersistableRenderedImage;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureCountMinSketchStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureCountMinSketchStatistics.FeatureCountMinSketchConfig;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureFixedBinNumericStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureFixedBinNumericStatistics.FeatureFixedBinConfig;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureHyperLogLogStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureHyperLogLogStatistics.FeatureHyperLogLogConfig;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericHistogramStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericHistogramStatistics.FeatureNumericHistogramConfig;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericRangeStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericRangeStatistics.FeatureNumericRangeConfig;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureTimeRangeStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.StatsConfigurationCollection;
import mil.nga.giat.geowave.adapter.vector.stats.StatsConfigurationCollection.SimpleFeatureStatsConfigurationCollection;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors.TimeDescriptorConfiguration;
import mil.nga.giat.geowave.core.index.persist.PersistableRegistrySpi;

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
					CQLQuery::new),
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
					TextSecondaryIndexConfiguration::new)
		};
	}
}
