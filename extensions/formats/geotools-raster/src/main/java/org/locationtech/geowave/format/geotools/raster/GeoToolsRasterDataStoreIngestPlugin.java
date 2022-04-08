/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.geotools.raster;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FilenameUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.geotools.referencing.CRS;
import org.geotools.util.factory.Hints;
import org.locationtech.geowave.adapter.raster.RasterUtils;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.plugin.GeoWaveGTRasterFormat;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialField;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIterator.Wrapper;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.internal.Maps;

/**
 * This plugin is used for ingesting any GeoTools supported file data store from a local file system
 * directly into GeoWave as GeoTools' SimpleFeatures. It supports the default configuration of
 * spatial and spatial-temporal indices and does NOT currently support the capability to stage
 * intermediate data to HDFS to be ingested using a map-reduce job.
 */
public class GeoToolsRasterDataStoreIngestPlugin implements LocalFileIngestPlugin<GridCoverage> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoToolsRasterDataStoreIngestPlugin.class);
  private final RasterOptionProvider optionProvider;

  public GeoToolsRasterDataStoreIngestPlugin() {
    this(new RasterOptionProvider());
  }

  public GeoToolsRasterDataStoreIngestPlugin(final RasterOptionProvider optionProvider) {
    this.optionProvider = optionProvider;
  }

  @Override
  public String[] getFileExtensionFilters() {
    return new String[] {};
  }

  @Override
  public void init(final URL baseDirectory) {}

  @Override
  public boolean supportsFile(final URL file) {
    AbstractGridFormat format = null;
    try {
      format = GridFormatFinder.findFormat(file);
    } catch (final Exception e) {
      LOGGER.info("Unable to support as raster file", e);
    }
    // the null check is enough and we don't need to check the format
    // accepts this file because the finder should have previously validated
    // this, also don't allwo ingest from geowave raster format because its URL validation is way
    // too lenient (ie. the URL is probably not supported)
    return ((format != null) && !(format instanceof GeoWaveGTRasterFormat));
  }

  private static AbstractGridFormat prioritizedFindFormat(final URL input) {
    final AbstractGridFormat format = null;
    try {
      final Set<AbstractGridFormat> formats = GridFormatFinder.findFormats(input);
      if ((formats == null) || formats.isEmpty()) {
        LOGGER.warn("Unable to support raster file " + input.getPath());
        return null;
      }
      // world image and geotiff can both open tif files, give
      // priority to gdalgeotiff, followed by geotiff
      for (final AbstractGridFormat f : formats) {
        if ("GDALGeoTiff".equals(f.getName())) {
          return f;
        }
      }
      for (final AbstractGridFormat f : formats) {
        if ("GeoTIFF".equals(f.getName())) {
          return f;
        }
      }

      // otherwise just pick the first
      final Iterator<AbstractGridFormat> it = formats.iterator();
      if (it.hasNext()) {
        return it.next();
      }
    } catch (final Exception e) {
      LOGGER.warn("Error while trying read raster file", e);
      return null;
    }
    return format;
  }

  @Override
  public CloseableIterator<GeoWaveData<GridCoverage>> toGeoWaveData(
      final URL input,
      final String[] indexNames) {
    final AbstractGridFormat format = prioritizedFindFormat(input);
    if (format == null) {
      return new Wrapper<>(Collections.emptyIterator());
    }
    Hints hints = null;
    if ((optionProvider.getCrs() != null) && !optionProvider.getCrs().trim().isEmpty()) {
      try {
        final CoordinateReferenceSystem crs = CRS.decode(optionProvider.getCrs());
        if (crs != null) {
          hints = new Hints();
          hints.put(Hints.DEFAULT_COORDINATE_REFERENCE_SYSTEM, crs);
        }
      } catch (final Exception e) {
        LOGGER.warn("Unable to find coordinate reference system, continuing without hint", e);
      }
    }
    final GridCoverage2DReader reader = format.getReader(input, hints);
    if (reader == null) {
      LOGGER.error("Unable to get reader instance, getReader returned null");
      return new Wrapper<>(Collections.emptyIterator());
    }
    try {
      final GridCoverage2D coverage = reader.read(null);
      if (coverage != null) {
        final Map<String, String> metadata = new HashMap<>();
        final String coverageName = coverage.getName().toString();
        try {
          // wrapping with try-catch block because often the reader
          // does not support operations on coverage name
          // if not, we just don't have metadata, and continue
          final String[] mdNames = reader.getMetadataNames(coverageName);
          if ((mdNames != null) && (mdNames.length > 0)) {
            for (final String mdName : mdNames) {
              if (mdName != null) {
                final String value = reader.getMetadataValue(coverageName, mdName);
                if (value != null) {
                  metadata.put(mdName, value);
                }
              }
            }
          }
        } catch (final Exception e) {
          LOGGER.debug("Unable to find metadata from coverage reader", e);
        }
        final List<GeoWaveData<GridCoverage>> coverages = new ArrayList<>();

        if (optionProvider.isSeparateBands() && (coverage.getNumSampleDimensions() > 1)) {
          final String baseName =
              optionProvider.getCoverageName() != null ? optionProvider.getCoverageName()
                  : FilenameUtils.getName(input.getPath());
          final double[][] nodata = optionProvider.getNodata(coverage.getNumSampleDimensions());
          for (int b = 0; b < coverage.getNumSampleDimensions(); b++) {
            final RasterDataAdapter adapter =
                new RasterDataAdapter(
                    baseName + "_B" + b,
                    metadata,
                    (GridCoverage2D) RasterUtils.getCoverageOperations().selectSampleDimension(
                        coverage,
                        new int[] {b}),
                    optionProvider.getTileSize(),
                    optionProvider.isBuildPyramid(),
                    optionProvider.isBuildHistogram(),
                    new double[][] {nodata[b]});
            coverages.add(new GeoWaveData<>(adapter, indexNames, coverage));
          }
        } else {
          final RasterDataAdapter adapter =
              new RasterDataAdapter(
                  optionProvider.getCoverageName() != null ? optionProvider.getCoverageName()
                      : input.getPath(),
                  metadata,
                  coverage,
                  optionProvider.getTileSize(),
                  optionProvider.isBuildPyramid(),
                  optionProvider.isBuildHistogram(),
                  optionProvider.getNodata(coverage.getNumSampleDimensions()));
          coverages.add(new GeoWaveData<>(adapter, indexNames, coverage));
        }
        return new Wrapper<GeoWaveData<GridCoverage>>(coverages.iterator()) {

          @Override
          public void close() {
            try {
              reader.dispose();
            } catch (final IOException e) {
              LOGGER.warn("unable to dispose of reader resources", e);
            }
          }
        };
      } else {
        LOGGER.warn(
            "Null grid coverage from file '"
                + input.getPath()
                + "' for discovered geotools format '"
                + format.getName()
                + "'");
      }
    } catch (final IOException e) {
      LOGGER.warn(
          "Unable to read grid coverage of file '"
              + input.getPath()
              + "' for discovered geotools format '"
              + format.getName()
              + "'",
          e);
    }
    return new Wrapper<>(Collections.emptyIterator());
  }

  @Override
  public DataTypeAdapter<GridCoverage>[] getDataAdapters() {
    return new RasterDataAdapter[] {};
  }

  @Override
  public DataTypeAdapter<GridCoverage>[] getDataAdapters(final URL url) {
    final Map<String, DataTypeAdapter<GridCoverage>> adapters = Maps.newHashMap();
    try (CloseableIterator<GeoWaveData<GridCoverage>> dataIt = toGeoWaveData(url, new String[0])) {
      while (dataIt.hasNext()) {
        final DataTypeAdapter<GridCoverage> adapter = dataIt.next().getAdapter();
        adapters.put(adapter.getTypeName(), adapter);
      }
    }
    return adapters.values().toArray(new RasterDataAdapter[adapters.size()]);
  }

  @Override
  public Index[] getRequiredIndices() {
    return new Index[] {};
  }

  @Override
  public String[] getSupportedIndexTypes() {
    return new String[] {SpatialField.DEFAULT_GEOMETRY_FIELD_NAME};
  }
}
