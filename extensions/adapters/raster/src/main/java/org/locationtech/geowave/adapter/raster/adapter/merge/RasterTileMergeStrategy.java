/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.adapter.merge;

import java.awt.image.SampleModel;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.adapter.RasterTile;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.opengis.coverage.grid.GridCoverage;

public interface RasterTileMergeStrategy<T extends Persistable> extends Persistable {
  public void merge(RasterTile<T> thisTile, RasterTile<T> nextTile, SampleModel sampleModel);

  public T getMetadata(GridCoverage tileGridCoverage, RasterDataAdapter dataAdapter);
}
