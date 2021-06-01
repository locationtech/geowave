/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.plugin.gdal;

import org.geotools.coverageio.gdal.BaseGDALGridCoverage2DReader;
import org.geotools.coverageio.gdal.dted.DTEDReader;
import org.geotools.data.DataSourceException;
import org.geotools.util.factory.Hints;
import org.opengis.coverage.grid.Format;
import org.opengis.coverage.grid.GridCoverageReader;
import it.geosolutions.imageio.plugins.geotiff.GeoTiffImageReaderSpi;

public class GDALGeoTiffReader extends BaseGDALGridCoverage2DReader implements GridCoverageReader {
  private static final String worldFileExt = "";

  /**
   * Creates a new instance of a {@link DTEDReader}. I assume nothing about file extension.
   *
   * @param input Source object for which we want to build an {@link DTEDReader} .
   * @throws DataSourceException
   */
  public GDALGeoTiffReader(final Object input) throws DataSourceException {
    this(input, null);
  }

  /**
   * Creates a new instance of a {@link DTEDReader}. I assume nothing about file extension.
   *
   * @param input Source object for which we want to build an {@link DTEDReader} .
   * @param hints Hints to be used by this reader throughout his life.
   * @throws DataSourceException
   */
  public GDALGeoTiffReader(final Object input, final Hints hints) throws DataSourceException {
    super(input, hints, worldFileExt, new GeoTiffImageReaderSpi());
  }

  /** @see org.opengis.coverage.grid.GridCoverageReader#getFormat() */
  @Override
  public Format getFormat() {
    return new GDALGeoTiffFormat();
  }
}
