/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.plugin.gdal;

import java.util.Collections;
import java.util.HashMap;
import org.geotools.coverageio.gdal.BaseGDALGridFormat;
import org.geotools.data.DataSourceException;
import org.geotools.parameter.DefaultParameterDescriptorGroup;
import org.geotools.parameter.ParameterGroup;
import org.geotools.util.factory.Hints;
import org.opengis.coverage.grid.Format;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.parameter.GeneralParameterDescriptor;
import it.geosolutions.imageio.plugins.geotiff.GeoTiffImageReaderSpi;

public class GDALGeoTiffFormat extends BaseGDALGridFormat implements Format {

  /** Creates an instance and sets the metadata. */
  public GDALGeoTiffFormat() {
    super(new GeoTiffImageReaderSpi());

    setInfo();
  }

  /** Sets the metadata information. */
  @Override
  protected void setInfo() {
    final HashMap<String, String> info = new HashMap<>();
    info.put("name", "GDALGeoTiff");
    info.put("description", "GDAL GeoTiff Coverage Format");
    info.put("vendor", "GeoWave");
    info.put("docURL", ""); // TODO: set something
    info.put("version", "1.0");
    mInfo = Collections.unmodifiableMap(info);

    // writing parameters
    writeParameters = null;

    // reading parameters
    readParameters =
        new ParameterGroup(
            new DefaultParameterDescriptorGroup(
                mInfo,
                new GeneralParameterDescriptor[] {
                    READ_GRIDGEOMETRY2D,
                    USE_JAI_IMAGEREAD,
                    USE_MULTITHREADING,
                    SUGGESTED_TILE_SIZE}));
  }

  @Override
  public GDALGeoTiffReader getReader(final Object source, final Hints hints) {
    try {
      return new GDALGeoTiffReader(source, hints);
    } catch (final MismatchedDimensionException e) {
      final RuntimeException re = new RuntimeException();
      re.initCause(e);
      throw re;
    } catch (final DataSourceException e) {
      final RuntimeException re = new RuntimeException();
      re.initCause(e);
      throw re;
    }
  }
}
