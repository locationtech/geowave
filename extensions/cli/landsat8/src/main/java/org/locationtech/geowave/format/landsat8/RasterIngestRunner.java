/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.landsat8;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.processing.AbstractOperation;
import org.geotools.coverage.processing.CoverageProcessor;
import org.geotools.coverage.processing.operation.BandMerge;
import org.geotools.coverage.processing.operation.BandMerge.TransformList;
import org.geotools.coverage.processing.operation.Crop;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.adapter.raster.RasterUtils;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import org.locationtech.geowave.adapter.raster.plugin.gdal.GDALGeoTiffReader;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.geotime.util.ExtractGeometryFilterVisitor;
import org.locationtech.geowave.core.geotime.util.ExtractGeometryFilterVisitorResult;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.store.StoreLoader;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.parameter.InvalidParameterValueException;
import org.opengis.parameter.ParameterNotFoundException;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ui.freemarker.FreeMarkerTemplateUtils;
import com.beust.jcommander.ParameterException;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import it.geosolutions.jaiext.range.RangeFactory;

public class RasterIngestRunner extends DownloadRunner {
  private static final double LANDSAT8_NO_DATA_VALUE_BQA = 1;
  private static final double LANDSAT8_NO_DATA_VALUE_OTHER_BANDS = 0;

  private static final Logger LOGGER = LoggerFactory.getLogger(RasterIngestRunner.class);
  private static Map<String, Landsat8BandConverterSpi> registeredBandConverters = null;
  protected final List<String> parameters;
  protected Landsat8RasterIngestCommandLineOptions ingestOptions;
  protected List<SimpleFeature> lastSceneBands = new ArrayList<>();
  protected Template coverageNameTemplate;
  protected final Map<String, Writer> writerCache = new HashMap<>();

  protected String[] bandsIngested;
  protected DataStore store = null;
  protected DataStorePluginOptions dataStorePluginOptions = null;
  protected Index[] indices = null;

  public RasterIngestRunner(
      final Landsat8BasicCommandLineOptions analyzeOptions,
      final Landsat8DownloadCommandLineOptions downloadOptions,
      final Landsat8RasterIngestCommandLineOptions ingestOptions,
      final List<String> parameters) {
    super(analyzeOptions, downloadOptions);
    this.ingestOptions = ingestOptions;
    this.parameters = parameters;
  }

  protected void processParameters(final OperationParams params) throws Exception {
    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException("Requires arguments: <store name> <comma delimited index list>");
    }
    final String inputStoreName = parameters.get(0);
    final String indexList = parameters.get(1);

    // Config file
    final File configFile = (File) params.getContext().get(ConfigOptions.PROPERTIES_FILE_CONTEXT);

    // Attempt to load input store.
    final StoreLoader inputStoreLoader = new StoreLoader(inputStoreName);
    if (!inputStoreLoader.loadFromConfig(configFile)) {
      throw new ParameterException("Cannot find store name: " + inputStoreLoader.getStoreName());
    }
    dataStorePluginOptions = inputStoreLoader.getDataStorePlugin();
    store = dataStorePluginOptions.createDataStore();

    // Load the Indices
    indices =
        DataStoreUtils.loadIndices(dataStorePluginOptions.createIndexStore(), indexList).toArray(
            new Index[0]);

    coverageNameTemplate =
        new Template(
            "name",
            new StringReader(ingestOptions.getCoverageName()),
            new Configuration());
  }

  @Override
  protected void runInternal(final OperationParams params) throws Exception {
    try {
      processParameters(params);
      super.runInternal(params);
    } finally {
      for (final Writer writer : writerCache.values()) {
        if (writer != null) {
          writer.close();
        }
      }
    }
  }

  protected BandData getBandData(final SimpleFeature band) throws IOException, TemplateException {
    final Map<String, Object> model = new HashMap<>();
    final SimpleFeatureType type = band.getFeatureType();
    for (final AttributeDescriptor attr : type.getAttributeDescriptors()) {
      final String attrName = attr.getLocalName();
      final Object attrValue = band.getAttribute(attrName);
      if (attrValue != null) {
        model.put(attrName, attrValue);
      }
    }
    final String coverageName =
        FreeMarkerTemplateUtils.processTemplateIntoString(coverageNameTemplate, model);
    final File geotiffFile = DownloadRunner.getDownloadFile(band, landsatOptions.getWorkspaceDir());
    final GDALGeoTiffReader reader = new GDALGeoTiffReader(geotiffFile);
    GridCoverage2D coverage = reader.read(null);
    reader.dispose();
    if ((ingestOptions.getCoverageConverter() != null)
        && !ingestOptions.getCoverageConverter().trim().isEmpty()) {
      // a converter was supplied, attempt to use it
      final Landsat8BandConverterSpi converter = getConverter(ingestOptions.getCoverageConverter());
      if (converter != null) {
        coverage = converter.convert(coverageName, coverage, band);
      }
    }
    if (ingestOptions.isSubsample()) {
      coverage =
          (GridCoverage2D) RasterUtils.getCoverageOperations().filteredSubsample(
              coverage,
              ingestOptions.getScale(),
              ingestOptions.getScale(),
              null);
    }
    // its unclear whether cropping should be done first or subsampling
    if (ingestOptions.isCropToSpatialConstraint()) {
      boolean cropped = false;
      final Filter filter = landsatOptions.getCqlFilter();
      if (filter != null) {
        final ExtractGeometryFilterVisitorResult geometryAndCompareOp =
            ExtractGeometryFilterVisitor.getConstraints(
                filter,
                GeometryUtils.getDefaultCRS(),
                SceneFeatureIterator.SHAPE_ATTRIBUTE_NAME);
        Geometry geometry = geometryAndCompareOp.getGeometry();
        if (geometry != null) {
          // go ahead and intersect this with the scene geometry
          final Geometry sceneShape =
              (Geometry) band.getAttribute(SceneFeatureIterator.SHAPE_ATTRIBUTE_NAME);
          if (geometry.contains(sceneShape)) {
            cropped = true;
          } else {
            geometry = geometry.intersection(sceneShape);
            final CoverageProcessor processor = CoverageProcessor.getInstance();
            final AbstractOperation op = (AbstractOperation) processor.getOperation("CoverageCrop");
            final ParameterValueGroup params = op.getParameters();
            params.parameter("Source").setValue(coverage);
            try {
              final MathTransform transform =
                  CRS.findMathTransform(
                      GeometryUtils.getDefaultCRS(),
                      coverage.getCoordinateReferenceSystem(),
                      true);
              params.parameter(Crop.CROP_ROI.getName().getCode()).setValue(
                  JTS.transform(geometry, transform));
              final double nodataValue = getNoDataValue(band);
              params.parameter(Crop.NODATA.getName().getCode()).setValue(
                  RangeFactory.create(nodataValue, nodataValue));

              params.parameter(Crop.DEST_NODATA.getName().getCode()).setValue(
                  new double[] {nodataValue});
              coverage = (GridCoverage2D) op.doOperation(params, null);
              cropped = true;
            } catch (InvalidParameterValueException | ParameterNotFoundException | FactoryException
                | MismatchedDimensionException | TransformException e) {
              LOGGER.warn("Unable to crop image", e);
            }
          }
        }
        if (!cropped) {
          LOGGER.warn(
              "Option to crop spatially was set but no spatial constraints were provided in CQL expression");
        }
      }
    }
    return new BandData(coverageName, coverage, reader, geotiffFile);
  }

  private static double getNoDataValue(final SimpleFeature band) {
    final String bandName = band.getAttribute(BandFeatureIterator.BAND_ATTRIBUTE_NAME).toString();
    return getNoDataValueFromName(bandName);
  }

  public static double getNoDataValueFromName(final String bandName) {
    double nodataValue;
    if ("BQA".equals(bandName)) {
      nodataValue = LANDSAT8_NO_DATA_VALUE_BQA;
    } else {
      nodataValue = LANDSAT8_NO_DATA_VALUE_OTHER_BANDS;
    }
    return nodataValue;
  }

  @Override
  protected void nextBand(final SimpleFeature band, final AnalysisInfo analysisInfo) {
    super.nextBand(band, analysisInfo);
    if (ingestOptions.isCoveragePerBand()) {
      // ingest this band
      // convert the simplefeature into a map to resolve the coverage name
      // using a user supplied freemarker template

      try {
        final BandData bandData = getBandData(band);
        final GridCoverage2D coverage = bandData.coverage;
        final String coverageName = bandData.name;
        final GDALGeoTiffReader reader = bandData.reader;
        Writer writer = writerCache.get(coverageName);
        final GridCoverage2D nextCov = coverage;
        if (writer == null) {
          final Map<String, String> metadata = new HashMap<>();
          final String[] mdNames = reader.getMetadataNames();
          if ((mdNames != null) && (mdNames.length > 0)) {
            for (final String mdName : mdNames) {
              metadata.put(mdName, reader.getMetadataValue(mdName));
            }
          }

          final double nodataValue = getNoDataValue(band);
          final RasterDataAdapter adapter =
              new RasterDataAdapter(
                  coverageName,
                  metadata,
                  nextCov,
                  ingestOptions.getTileSize(),
                  ingestOptions.isCreatePyramid(),
                  ingestOptions.isCreateHistogram(),
                  new double[][] {new double[] {nodataValue}},
                  new NoDataMergeStrategy());
          store.addType(adapter, indices);
          writer = store.createWriter(adapter.getTypeName());
          writerCache.put(coverageName, writer);
        }
        writer.write(nextCov);
        if (!ingestOptions.isRetainImages()) {
          if (!bandData.geotiffFile.delete()) {
            LOGGER.warn("Unable to delete '" + bandData.geotiffFile.getAbsolutePath() + "'");
          }
        }
      } catch (IOException | TemplateException e) {
        LOGGER.error(
            "Unable to ingest band "
                + band.getID()
                + " because coverage name cannot be resolved from template",
            e);
      }
    } else {
      lastSceneBands.add(band);
    }
  }

  @Override
  protected void lastSceneComplete(final AnalysisInfo analysisInfo) {
    processPreviousScene();
    super.lastSceneComplete(analysisInfo);
    if (!ingestOptions.isSkipMerge()) {
      System.out.println("Merging overlapping tiles...");
      for (final Index index : indices) {
        if (dataStorePluginOptions.createDataStoreOperations().mergeData(
            index,
            dataStorePluginOptions.createAdapterStore(),
            dataStorePluginOptions.createInternalAdapterStore(),
            dataStorePluginOptions.createAdapterIndexMappingStore(),
            dataStorePluginOptions.getFactoryOptions().getStoreOptions().getMaxRangeDecomposition())) {
          System.out.println(
              "Successfully merged overlapping tiles within index '" + index.getName() + "'");
        } else {
          System.err.println(
              "Unable to merge overlapping landsat8 tiles in index '" + index.getName() + "'");
        }
      }
    }
  }

  @Override
  protected void nextScene(final SimpleFeature firstBandOfScene, final AnalysisInfo analysisInfo) {
    processPreviousScene();
    super.nextScene(firstBandOfScene, analysisInfo);
  }

  protected void processPreviousScene() {
    if (!ingestOptions.isCoveragePerBand()) {
      // ingest as single image for all bands
      if (!lastSceneBands.isEmpty()) {
        // we are sorting by band name to ensure a consistent order for
        // bands
        final TreeMap<String, BandData> sceneData = new TreeMap<>();
        Writer writer;
        // get coverage info, ensuring that all coverage names are the
        // same
        String coverageName = null;
        for (final SimpleFeature band : lastSceneBands) {
          BandData bandData;
          try {
            bandData = getBandData(band);
            if (coverageName == null) {
              coverageName = bandData.name;
            } else if (!coverageName.equals(bandData.name)) {
              LOGGER.warn(
                  "Unable to use band data as the band coverage name '"
                      + bandData.name
                      + "' is unexpectedly different from default name '"
                      + coverageName
                      + "'");
            }

            final String bandName =
                band.getAttribute(BandFeatureIterator.BAND_ATTRIBUTE_NAME).toString();
            sceneData.put(bandName, bandData);
          } catch (IOException | TemplateException e) {
            LOGGER.warn("Unable to read band data", e);
          }
        }
        if (coverageName == null) {
          LOGGER.warn("No valid bands found for scene");
          lastSceneBands.clear();
          return;
        }
        final GridCoverage2D mergedCoverage;
        if (sceneData.size() == 1) {
          mergedCoverage = sceneData.firstEntry().getValue().coverage;
        } else {
          final CoverageProcessor processor = CoverageProcessor.getInstance();
          final AbstractOperation op = (AbstractOperation) processor.getOperation("BandMerge");
          final ParameterValueGroup params = op.getParameters();
          final List<GridCoverage2D> sources = new ArrayList<>();
          for (final BandData b : sceneData.values()) {
            sources.add(b.coverage);
          }
          params.parameter("Sources").setValue(sources);
          params.parameter(BandMerge.TRANSFORM_CHOICE).setValue(TransformList.FIRST.toString());

          mergedCoverage = (GridCoverage2D) op.doOperation(params, null);
        }
        final String[] thisSceneBands = sceneData.keySet().toArray(new String[] {});
        if (bandsIngested == null) {
          // this means this is the first scene
          // setup adapter and other required info
          final Map<String, String> metadata = new HashMap<>();
          // merge metadata from all readers
          for (final BandData b : sceneData.values()) {
            final String[] mdNames = b.reader.getMetadataNames();
            if ((mdNames != null) && (mdNames.length > 0)) {
              for (final String mdName : mdNames) {
                metadata.put(mdName, b.reader.getMetadataValue(mdName));
              }
            }
          }
          final double[][] noDataValues = new double[sceneData.size()][];
          int b = 0;
          for (final String bandName : sceneData.keySet()) {
            noDataValues[b++] = new double[] {getNoDataValueFromName(bandName)};
          }
          final RasterDataAdapter adapter =
              new RasterDataAdapter(
                  coverageName,
                  metadata,
                  mergedCoverage,
                  ingestOptions.getTileSize(),
                  ingestOptions.isCreatePyramid(),
                  ingestOptions.isCreateHistogram(),
                  noDataValues,
                  new NoDataMergeStrategy());
          store.addType(adapter, indices);
          writer = store.createWriter(adapter.getTypeName());
          writerCache.put(coverageName, writer);
          bandsIngested = thisSceneBands;
        } else if (!Arrays.equals(bandsIngested, thisSceneBands)) {
          LOGGER.warn(
              "The bands in this scene ('"
                  + Arrays.toString(thisSceneBands)
                  + "') differ from the previous scene ('"
                  + Arrays.toString(bandsIngested)
                  + "').  To merge bands all scenes must use the same bands.  Skipping scene'"
                  + lastSceneBands.get(0).getAttribute(
                      SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME)
                  + "'.");
          lastSceneBands.clear();
          return;
        } else {
          writer = writerCache.get(coverageName);
          if (writer == null) {
            LOGGER.warn(
                "Unable to find writer for coverage '"
                    + coverageName
                    + "'.  Skipping scene'"
                    + lastSceneBands.get(0).getAttribute(
                        SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME)
                    + "'.");
            lastSceneBands.clear();
            return;
          }
        }
        writer.write(mergedCoverage);
        lastSceneBands.clear();
        if (!ingestOptions.isRetainImages()) {
          for (final BandData b : sceneData.values()) {
            if (!b.geotiffFile.delete()) {
              LOGGER.warn("Unable to delete '" + b.geotiffFile.getAbsolutePath() + "'");
            }
          }
        }
      }
    }
  }

  public Landsat8BandConverterSpi getConverter(final String converterName) {
    final Landsat8BandConverterSpi converter = getRegisteredConverters().get(converterName);
    if (converter == null) {
      LOGGER.warn("no landsat8 converter registered with name '" + converterName + "'");
    }
    return converter;
  }

  private synchronized Map<String, Landsat8BandConverterSpi> getRegisteredConverters() {
    if (registeredBandConverters == null) {
      registeredBandConverters = new HashMap<>();
      final ServiceLoader<Landsat8BandConverterSpi> converters =
          ServiceLoader.load(Landsat8BandConverterSpi.class);
      final Iterator<Landsat8BandConverterSpi> it = converters.iterator();
      while (it.hasNext()) {
        final Landsat8BandConverterSpi converter = it.next();
        registeredBandConverters.put(converter.getName(), converter);
      }
    }
    return registeredBandConverters;
  }

  private static class BandData {
    private final String name;
    private final GridCoverage2D coverage;
    private final GDALGeoTiffReader reader;
    private final File geotiffFile;

    public BandData(
        final String name,
        final GridCoverage2D coverage,
        final GDALGeoTiffReader reader,
        final File geotiffFile) {
      this.name = name;
      this.coverage = coverage;
      this.reader = reader;
      this.geotiffFile = geotiffFile;
    }
  }
}
