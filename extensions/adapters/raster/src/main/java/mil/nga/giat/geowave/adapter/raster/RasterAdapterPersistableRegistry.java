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
package mil.nga.giat.geowave.adapter.raster;

import mil.nga.giat.geowave.adapter.raster.adapter.CompoundHierarchicalIndexStrategyWrapper;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterTile;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.RasterTileRowTransform;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.RootMergeStrategy;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataByFilter;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataBySampleIndex;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import mil.nga.giat.geowave.adapter.raster.stats.HistogramConfig;
import mil.nga.giat.geowave.adapter.raster.stats.HistogramStatistics;
import mil.nga.giat.geowave.adapter.raster.stats.OverviewStatistics;
import mil.nga.giat.geowave.adapter.raster.stats.RasterBoundingBoxStatistics;
import mil.nga.giat.geowave.adapter.raster.stats.RasterFootprintStatistics;
import mil.nga.giat.geowave.core.index.persist.PersistableRegistrySpi;

public class RasterAdapterPersistableRegistry implements
		PersistableRegistrySpi
{

	@Override
	public PersistableIdAndConstructor[] getSupportedPersistables() {
		return new PersistableIdAndConstructor[] {
			new PersistableIdAndConstructor(
					(short) 600,
					Resolution::new),
			new PersistableIdAndConstructor(
					(short) 601,
					CompoundHierarchicalIndexStrategyWrapper::new),
			new PersistableIdAndConstructor(
					(short) 602,
					RasterDataAdapter::new),
			new PersistableIdAndConstructor(
					(short) 603,
					RasterTile::new),
			new PersistableIdAndConstructor(
					(short) 604,
					RasterTileRowTransform::new),
			new PersistableIdAndConstructor(
					(short) 605,
					RootMergeStrategy::new),
			new PersistableIdAndConstructor(
					(short) 606,
					NoDataByFilter::new),
			new PersistableIdAndConstructor(
					(short) 607,
					NoDataBySampleIndex::new),
			new PersistableIdAndConstructor(
					(short) 608,
					NoDataMergeStrategy::new),
			new PersistableIdAndConstructor(
					(short) 609,
					HistogramConfig::new),
			new PersistableIdAndConstructor(
					(short) 610,
					HistogramStatistics::new),
			new PersistableIdAndConstructor(
					(short) 611,
					OverviewStatistics::new),
			new PersistableIdAndConstructor(
					(short) 612,
					RasterBoundingBoxStatistics::new),
			new PersistableIdAndConstructor(
					(short) 613,
					RasterFootprintStatistics::new),
		};
	}
}
