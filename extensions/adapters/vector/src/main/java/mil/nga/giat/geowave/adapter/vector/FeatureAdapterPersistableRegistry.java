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
package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.adapter.vector.index.SecondaryIndexManager;
import mil.nga.giat.geowave.adapter.vector.ingest.CQLFilterOptionProvider;
import mil.nga.giat.geowave.adapter.vector.ingest.DataSchemaOptionProvider;
import mil.nga.giat.geowave.adapter.vector.ingest.FeatureSerializationOptionProvider;
import mil.nga.giat.geowave.adapter.vector.ingest.TypeNameOptionProvider;
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
import mil.nga.giat.geowave.adapter.vector.stats.FeatureFixedBinNumericStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureHyperLogLogStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericHistogramStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericRangeStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureTimeRangeStatistics;
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
					DistributedRenderAggregation::new)
		};
	}
}
