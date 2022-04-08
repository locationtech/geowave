/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster;

import org.locationtech.geowave.adapter.raster.adapter.ClientMergeableRasterTile;
import org.locationtech.geowave.adapter.raster.adapter.InternalRasterDataAdapter;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.adapter.RasterTile;
import org.locationtech.geowave.adapter.raster.adapter.ServerMergeableRasterTile;
import org.locationtech.geowave.adapter.raster.adapter.merge.MultiAdapterServerMergeStrategy;
import org.locationtech.geowave.adapter.raster.adapter.merge.RasterTileRowTransform;
import org.locationtech.geowave.adapter.raster.adapter.merge.SingleAdapterServerMergeStrategy;
import org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataByFilter;
import org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataBySampleIndex;
import org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import org.locationtech.geowave.adapter.raster.stats.HistogramConfig;
import org.locationtech.geowave.core.index.persist.InternalPersistableRegistry;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;
import org.locationtech.geowave.core.store.util.CompoundHierarchicalIndexStrategyWrapper;

public class RasterAdapterPersistableRegistry implements
    PersistableRegistrySpi,
    InternalPersistableRegistry {

  @Override
  public PersistableIdAndConstructor[] getSupportedPersistables() {
    return new PersistableIdAndConstructor[] {
        new PersistableIdAndConstructor((short) 600, Resolution::new),
        new PersistableIdAndConstructor((short) 601, CompoundHierarchicalIndexStrategyWrapper::new),
        new PersistableIdAndConstructor((short) 602, RasterDataAdapter::new),
        new PersistableIdAndConstructor((short) 603, RasterTile::new),
        new PersistableIdAndConstructor((short) 604, RasterTileRowTransform::new),
        new PersistableIdAndConstructor((short) 605, MultiAdapterServerMergeStrategy::new),
        new PersistableIdAndConstructor((short) 606, NoDataByFilter::new),
        new PersistableIdAndConstructor((short) 607, NoDataBySampleIndex::new),
        new PersistableIdAndConstructor((short) 608, NoDataMergeStrategy::new),
        new PersistableIdAndConstructor((short) 609, HistogramConfig::new),
        new PersistableIdAndConstructor((short) 614, ServerMergeableRasterTile::new),
        new PersistableIdAndConstructor((short) 615, SingleAdapterServerMergeStrategy::new),
        new PersistableIdAndConstructor((short) 616, ClientMergeableRasterTile::new),
        // 617 used by RasterRegisteredIndexFieldMappers
        new PersistableIdAndConstructor((short) 618, InternalRasterDataAdapter::new)};
  }
}
