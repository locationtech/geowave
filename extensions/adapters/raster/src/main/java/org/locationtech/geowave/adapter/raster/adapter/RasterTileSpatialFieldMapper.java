/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.adapter;

import java.util.List;
import org.locationtech.geowave.core.geotime.adapter.SpatialFieldMapper;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.locationtech.jts.geom.Geometry;

/**
 * An index field mapper for `RasterTiles`. This class does not actually do any mapping because the
 * mapping is handled by special logic in the adapter. Never the less, it is needed so that GeoWave
 * is able to map the raster data adapter to a spatial index.
 */
public class RasterTileSpatialFieldMapper extends SpatialFieldMapper<RasterTile> {

  @Override
  protected Geometry getNativeGeometry(List<RasterTile> nativeFieldValues) {
    // Unused, since adapter handles the mapping manually
    return null;
  }

  @Override
  public void toAdapter(final Geometry indexFieldValue, final RowBuilder<?> rowBuilder) {
    // Unused, since adapter handles the mapping manually
  }

  @Override
  public Class<RasterTile> adapterFieldType() {
    return RasterTile.class;
  }

  @Override
  public short adapterFieldCount() {
    return 1;
  }

}
