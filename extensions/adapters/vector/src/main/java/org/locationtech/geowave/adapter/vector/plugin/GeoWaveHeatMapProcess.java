/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 * 
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin;

import java.io.IOException;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.vector.BBOXExpandingFilterVisitor;
import org.geotools.process.vector.BilinearInterpolator;
import org.geotools.process.vector.HeatmapSurface;
import org.geotools.process.vector.VectorProcess;
import org.geotools.referencing.CRS;
import org.geotools.util.factory.GeoTools;
import org.geotools.util.factory.Hints;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.util.Stopwatch;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.coverage.grid.GridGeometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.expression.Expression;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.util.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Process that uses a {@link HeatmapSurface} to compute a heatmap surface over a set of irregular
 * data points as a {@link GridCoverage}. Heatmaps are known more formally as <i>Multivariate Kernel
 * Density Estimation</i>.
 *
 * <p>The appearance of the heatmap is controlled by the kernel radius, which determines the "radius
 * of influence" of input points. The radius is specified by the radiusPixels parameter, which is in
 * output pixels. Using pixels allows easy estimation of a value which will give a visually
 * effective result, and ensures the heatmap appearance changes to match the zoom level.
 *
 * <p>By default each input point has weight 1. Optionally the weights of points may be supplied by
 * an attribute specified by the <code>weightAttr</code> parameter.
 *
 * <p>All geometry types are allowed as input. For non-point geometries the centroid is used.
 *
 * <p>To improve performance, the surface grid can be computed at a lower resolution than the
 * requested output image using the <code>pixelsPerCell</code> parameter. The grid is upsampled to
 * match the required image size. Upsampling uses Bilinear Interpolation to maintain visual quality.
 * This gives a large improvement in performance, with minimal impact on visual quality for small
 * cell sizes (for instance, 10 pixels or less).
 *
 * <p>To ensure that the computed surface is stable (i.e. does not display obvious edge artifacts
 * under zooming and panning), the data extent is expanded to be larger than the specified output
 * extent. The expansion distance is equal to the size of <code>radiusPixels</code> in the input
 * CRS.
 *
 * <h3>Parameters</h3>
 *
 * <i>M = mandatory, O = optional</i>
 *
 * <ul> <li><b>data</b> (M) - the FeatureCollection containing the point observations
 * <li><b>radiusPixels</b> (M)- the density kernel radius, in pixels <li><b>weightAttr</b> (M)- the
 * feature type attribute containing the observed surface value <li><b>pixelsPerCell</b> (O) - The
 * pixels-per-cell value determines the resolution of the computed grid. Larger values improve
 * performance, but degrade appearance. (Default = 1) <li><b>outputBBOX</b> (M) - The georeferenced
 * bounding box of the output area <li><b>outputWidth</b> (M) - The width of the output raster
 * <li><b>outputHeight</b> (M) - The height of the output raster </ul>
 *
 * The output of the process is a {@linkplain GridCoverage2D} with a single band, with cell values
 * in the range [0, 1].
 *
 * <p>Computation of the surface takes places in the CRS of the output. If the data CRS is different
 * to the output CRS, the input points are transformed into the output CRS.
 *
 * <h3>Using the process as a Rendering Transformation</h3>
 *
 * This process can be used as a RenderingTransformation, since it implements the
 * <tt>invertQuery(... Query, GridGeometry)</tt> method. In this case the <code>queryBuffer</code>
 * parameter should be specified to expand the query extent appropriately. The output raster
 * parameters may be provided from the request extents, using the following SLD environment
 * variables:
 *
 * <ul> <li><b>outputBBOX</b> - env var = <tt>wms_bbox</tt> <li><b>outputWidth</b> - env var =
 * <tt>wms_width</tt> <li><b>outputHeight</b> - env var = <tt>wms_height</tt> </ul>
 *
 * When used as an Rendering Transformation the data query is rewritten to expand the query BBOX, to
 * ensure that enough data points are queried to make the computed surface stable under panning and
 * zooming.
 *
 * <p>
 * 
 * @author M. Zagorski (customizations for GeoWave Heatmap rendering using aggregation and statistic
 *         spatial binning queries).<br>
 * @apiNode Note: based on the GeoTools version of HeatmapProcess by Martin Davis - OpenGeo.
 * @apiNote Date: 3-25-2022 <br>
 *
 * @apiNote Changelog: <br>
 *
 *
 * 
 */
@SuppressWarnings("deprecation")
@DescribeProcess(
    title = "GeoWaveHeatMapProcess",
    description = "Computes a heatmap surface over a set of data points and outputs as a single-band raster.")
public class GeoWaveHeatMapProcess implements VectorProcess {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveHeatMapProcess.class);

  // For testing and verification of accuracy only (keep set to false in production)
  Boolean writeGeoJson = false;

  // Query types
  public static final String CNT_AGGR = "CNT_AGGR";
  public static final String SUM_AGGR = "SUM_AGGR";
  public static final String CNT_STATS = "CNT_STATS";
  public static final String SUM_STATS = "SUM_STATS";


  public static final Hints.Key HEATMAP_ENABLED = new Hints.Key(Boolean.class);
  public static final Hints.Key OUTPUT_BBOX = new Hints.Key(ReferencedEnvelope.class);
  public static final Hints.Key OUTPUT_WIDTH = new Hints.Key(Integer.class);
  public static final Hints.Key OUTPUT_HEIGHT = new Hints.Key(Integer.class);
  public static final Hints.Key GEOHASH_PREC = new Hints.Key(Integer.class);
  public static final Hints.Key AGGR_QUERY = new Hints.Key(Boolean.class);
  public static final Hints.Key STATS_QUERY = new Hints.Key(Boolean.class);
  public static final Hints.Key QUERY_TYPE = new Hints.Key(String.class);

  // The value of the weight attribute must be numeric (e.g. cannot be a geometry, etc.)
  public static final Hints.Key WEIGHT_ATTR = new Hints.Key(String.class);
  public static final Hints.Key PIXELS_PER_CELL = new Hints.Key(Integer.class);
  public static final Hints.Key CREATE_STATS = new Hints.Key(Boolean.class);
  public static final Hints.Key USE_BINNING = new Hints.Key(Boolean.class);


  @DescribeResult(name = "result", description = "Output raster")
  public GridCoverage2D execute(

      // process data
      @DescribeParameter(
          name = "data",
          description = "Input features") SimpleFeatureCollection obsFeatures,

      // process parameters
      @DescribeParameter(
          name = "radiusPixels",
          description = "Radius of the density kernel in pixels") Integer argRadiusPixels,
      @DescribeParameter(
          name = "weightAttr",
          description = "Name of the attribute to use for data point weight",
          min = 0,
          max = 1) String valueAttr,
      @DescribeParameter(
          name = "pixelsPerCell",
          description = "Resolution at which to compute the heatmap (in pixels). Default = 1",
          defaultValue = "1",
          min = 0,
          max = 1) Integer argPixelsPerCell,

      // output image parameters
      @DescribeParameter(
          name = "outputBBOX",
          description = "Bounding box of the output") ReferencedEnvelope argOutputEnv,
      @DescribeParameter(
          name = "outputWidth",
          description = "Width of output raster in pixels") Integer argOutputWidth,
      @DescribeParameter(
          name = "outputHeight",
          description = "Height of output raster in pixels") Integer argOutputHeight,

      // CUSTOM GEOWAVE PARAMETERS
      // Options for queryType include: CNT_AGGR, SUM_AGGR, CNT_STATS, SUM_STATS
      @DescribeParameter(
          name = "queryType",
          description = "Height of the output raster") String queryType,
      @DescribeParameter(
          name = "createStats",
          description = "Option to run statistics if they do not exist in datastore - must have queryType set to CNT_STATS or SUM_STATS.") Boolean createStats,
      @DescribeParameter(
          name = "useSpatialBinning",
          description = "Option to use spatial binning.") Boolean useSpatialBinning,


      ProgressListener monitor) throws ProcessException {

    /** -------- Extract required information from process arguments ------------- */
    int pixelsPerCell = 1;
    if (argPixelsPerCell != null && argPixelsPerCell > 1) {
      pixelsPerCell = argPixelsPerCell;
    }
    int outputWidth = argOutputWidth;
    int outputHeight = argOutputHeight;
    int gridWidth = outputWidth;
    int gridHeight = outputHeight;
    if (pixelsPerCell > 1) {
      gridWidth = outputWidth / pixelsPerCell;
      gridHeight = outputHeight / pixelsPerCell;
    }

    /** Compute transform to convert input coords into output CRS */
    CoordinateReferenceSystem srcCRS = obsFeatures.getSchema().getCoordinateReferenceSystem();
    CoordinateReferenceSystem dstCRS = argOutputEnv.getCoordinateReferenceSystem();

    System.out.println("HEATMAP - SOURCE CRS: " + srcCRS.getName());
    System.out.println("HEATMAP - DEST CRS: " + dstCRS.getName());

    // Boolean isWGS84 = dstCRS.getName().getCode().equals("WGS 84");
    // if (!isWGS84) {
    // // Decode the target CRS of "EPSG:4326"
    // try {
    // dstCRS = CRS.decode("EPSG:4326");
    // } catch (NoSuchAuthorityCodeException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // } catch (FactoryException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    // }

    System.out.println("HEATMAP - SOURCE CRS FINAL: " + srcCRS.getName());

    MathTransform trans = null;
    try {
      trans = CRS.findMathTransform(srcCRS, dstCRS);
    } catch (FactoryException e) {
      throw new ProcessException(e);
    }

    // ------------ Kernel Radius
    /*
     * // not used for now - only pixel radius values are supported double distanceConversionFactor
     * = distanceConversionFactor(srcCRS, dstCRS); double dstRadius = argRadius *
     * distanceConversionFactor;
     */
    int radiusCells = 100;
    if (argRadiusPixels != null)
      radiusCells = argRadiusPixels;
    if (pixelsPerCell > 1) {
      radiusCells /= pixelsPerCell;
    }

    /**
     * -------------- Extract the input observation points and add them to the heatmap -----------
     */
    HeatmapSurface heatMap = new HeatmapSurface(radiusCells, argOutputEnv, gridWidth, gridHeight);
    try {
      extractPoints(obsFeatures, valueAttr, trans, heatMap);
    } catch (CQLException e) {
      throw new ProcessException(e);
    }

    /** --------------- Do the processing on the heatmap------------------------------ */
    // KEEP the stopwatch for testing and verification purposes only
    // Stopwatch sw = new Stopwatch();

    // compute the heatmap at the specified resolution
    float[][] heatMapGrid = heatMap.computeSurface();

    // flip now, since grid size may be smaller
    heatMapGrid = flipXY(heatMapGrid);

    // upsample to output resolution if necessary
    float[][] outGrid = heatMapGrid;
    if (pixelsPerCell > 1)
      outGrid = upsample(heatMapGrid, -999, outputWidth, outputHeight);

    // convert to the GridCoverage2D required for output
    GridCoverageFactory gcf =
        CoverageFactoryFinder.getGridCoverageFactory(GeoTools.getDefaultHints());
    GridCoverage2D gridCov = gcf.create("Process Results", outGrid, argOutputEnv);

    // KEEP THIS System.out for testing and verification purposes only
    // System.out.println("************** Heatmap FINAL computed in " + sw.getTimeString());

    return gridCov;
  }

  /**
   * Flips an XY matrix along the X=Y axis, and inverts the Y axis. Used to convert from "map
   * orientation" into the "image orientation" used by GridCoverageFactory. The surface
   * interpolation is done on an XY grid, with Y=0 being the bottom of the space. GridCoverages are
   * stored in an image format, in a YX grid with Y=0 being the top.
   *
   * @param grid the grid to flip
   * @return the flipped grid
   */
  private float[][] flipXY(float[][] grid) {
    int xsize = grid.length;
    int ysize = grid[0].length;

    float[][] grid2 = new float[ysize][xsize];
    for (int ix = 0; ix < xsize; ix++) {
      for (int iy = 0; iy < ysize; iy++) {
        int iy2 = ysize - iy - 1;
        grid2[iy2][ix] = grid[ix][iy];
      }
    }
    return grid2;
  }

  private float[][] upsample(float[][] grid, float noDataValue, int width, int height) {
    BilinearInterpolator bi = new BilinearInterpolator(grid, noDataValue);
    float[][] outGrid = bi.interpolate(width, height, false);
    return outGrid;
  }

  /**
   * Given a target query and a target grid geometry returns the query to be used to read the input
   * data of the process involved in rendering. In this process this method is used to:
   *
   * <ul> <li>determine the extent & CRS of the output grid <li>expand the query envelope to ensure
   * stable surface generation <li>modify the query hints to ensure point features are returned
   * </ul>
   *
   * Note that in order to pass validation, all parameters named here must also appear in the
   * parameter list of the <tt>execute</tt> method, even if they are not used there.
   *
   * @param argRadiusPixels the feature type attribute that contains the observed surface value
   * @param targetQuery the query used against the data source
   * @param targetGridGeometry the grid geometry of the destination image
   * @return The transformed query
   */
  public Query invertQuery(
      @DescribeParameter(
          name = "radiusPixels",
          description = "Radius to use for the kernel",
          min = 0,
          max = 1) Integer argRadiusPixels,
      @DescribeParameter(
          name = "pixelsPerCell",
          description = "Resolution at which to compute the heatmap (in pixels). Default = 1",
          defaultValue = "1",
          min = 0,
          max = 1) Integer argPixelsPerCell,
      @DescribeParameter(
          name = "weightAttr",
          description = "Name of the attribute to use for data point weight",
          min = 0,
          max = 1) String valueAttr,
      // output image parameters
      @DescribeParameter(
          name = "outputBBOX",
          description = "Georeferenced bounding box of the output") ReferencedEnvelope argOutputEnv,
      @DescribeParameter(
          name = "outputWidth",
          description = "Width of the output raster") Integer argOutputWidth,
      @DescribeParameter(
          name = "outputHeight",
          description = "Height of the output raster") Integer argOutputHeight,
      // Can be: CNT_AGGR, SUM_AGGR, CNT_STATS, SUM_STATS
      @DescribeParameter(
          name = "queryType",
          description = "Height of the output raster") String queryType,
      @DescribeParameter(
          name = "createStats",
          description = "Option to run statistics if they do not exist in datastore - must have queryType set to CNT_STATS or SUM_STATS.") Boolean createStats,
      @DescribeParameter(
          name = "useSpatialBinning",
          description = "Option to use spatial binning.") Boolean useSpatialBinning,
      Query targetQuery,
      GridGeometry targetGridGeometry) throws ProcessException {

    // Get hints for this process
    Hints hints = targetQuery.getHints();

    // State that the hints for this process are enabled (for GeoWaveFeatureCollection.java)
    hints.put(HEATMAP_ENABLED, true);
    hints.put(PIXELS_PER_CELL, argPixelsPerCell);
    hints.put(OUTPUT_WIDTH, argOutputWidth);
    hints.put(OUTPUT_HEIGHT, argOutputHeight);
    hints.put(OUTPUT_BBOX, argOutputEnv);
    // hints.put(GEOHASH_PREC, 4);
    // hints.put(AGGR_QUERY, true);
    // hints.put(STATS_QUERY, false);

    // Add one of these values in the SLD: CNT_AGGR, SUM_AGGR, CNT_STATS, SUM_STATS.
    hints.put(QUERY_TYPE, queryType);

    hints.put(WEIGHT_ATTR, valueAttr);
    hints.put(CREATE_STATS, createStats);
    hints.put(USE_BINNING, useSpatialBinning);

    int radiusPixels = argRadiusPixels > 0 ? argRadiusPixels : 0;
    // input parameters are required, so should be non-null
    double queryBuffer = radiusPixels / pixelSize(argOutputEnv, argOutputWidth, argOutputHeight);
    /*
     * if (argQueryBuffer != null) { queryBuffer = argQueryBuffer; }
     */
    targetQuery.setFilter(expandBBox(targetQuery.getFilter(), queryBuffer));

    // clear properties to force all attributes to be read
    // (required because the SLD processor cannot see the value attribute specified in the
    // transformation)
    // TODO: set the properties to read only the specified value attribute
    targetQuery.setProperties(null);

    // set the decimation hint to ensure points are read
    // Hints hints = targetQuery.getHints();
    hints.put(Hints.GEOMETRY_DISTANCE, 0.0);

    return targetQuery;
  }

  private double pixelSize(ReferencedEnvelope outputEnv, int outputWidth, int outputHeight) {
    // error-proofing
    if (outputEnv.getWidth() <= 0)
      return 0;
    // assume view is isotropic
    return outputWidth / outputEnv.getWidth();
  }

  protected Filter expandBBox(Filter filter, double distance) {
    return (Filter) filter.accept(
        new BBOXExpandingFilterVisitor(distance, distance, distance, distance),
        null);
  }

  /**
   * Extract points from a feature collection, and stores them in the heatmap
   *
   * @param obsPoints features to extract
   * @param attrName expression or property name used to evaluate the geometry from a feature
   * @param trans transform for extracted points
   * @param heatMap heatmap to add points to
   * @throws CQLException if attrName can't be parsed
   */
  protected void extractPoints(
      SimpleFeatureCollection obsPoints,
      String attrName,
      MathTransform trans,
      HeatmapSurface heatMap) throws CQLException {

    Expression attrExpr = null;
    if (attrName != null) {
      attrExpr = ECQL.toExpression(attrName);
    }

    int counter = 0;

    try (SimpleFeatureIterator obsIt = obsPoints.features()) {
      double[] srcPt = new double[2];
      double[] dstPt = new double[2];

      // Iterate over the results
      while (obsIt.hasNext()) {
        SimpleFeature feature = obsIt.next();

        // try {
        // get the weight value, if any
        double val = 1;
        if (attrExpr != null) {
          val = getPointValue(feature, attrExpr);
        }

        // Get the information (testing and verification purposes only)
        if (writeGeoJson) {
          Expression geohashIdExpr = ECQL.toExpression("geohashId");
          String geohashId = geohashIdExpr.evaluate(feature, String.class);

          Expression sourceExpr = ECQL.toExpression("source");
          String source = sourceExpr.evaluate(feature, String.class);

          Expression geohashPrecExpr = ECQL.toExpression("geohashPrec");
          Integer geohashPrec = geohashPrecExpr.evaluate(feature, Integer.class);

          Expression fieldNameExpr = ECQL.toExpression("field_name");
          String fieldName = fieldNameExpr.evaluate(feature, String.class);

          // Create geojson file (for testing and verification purposes only)
          counter++;
          if (counter <= 30) {
            FeatureJSON fjson = new FeatureJSON();
            String name =
                "/home/milla/Desktop/BACKUP_WORKING/GEOWAVE_BACKUP/geowave/JOSM_Verification/output_data/"
                    + fieldName
                    + "_GEOHASH_"
                    + geohashPrec
                    + "_"
                    + geohashId
                    + "_"
                    + source
                    + "_val_"
                    + val
                    + ".geojson";
            try {
              fjson.writeFeature(feature, name);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }

        // get the point location from the geometry
        Geometry geom = (Geometry) feature.getDefaultGeometry();
        Coordinate p = getPoint(geom);
        srcPt[0] = p.x;
        srcPt[1] = p.y;

        try {
          trans.transform(srcPt, 0, dstPt, 0, 1);

          Coordinate pobs = new Coordinate(dstPt[0], dstPt[1], val);
          System.out.println("HEATMAP pobs: " + pobs);

          heatMap.addPoint(pobs.x, pobs.y, val);
        } catch (Exception e) {
          LOGGER.warn(
              "Expression {} failed to evaluate to a numeric value {} due to: {}",
              attrExpr,
              val,
              e);
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * Gets a point to represent the Geometry. If the Geometry is a point, this is returned.
   * Otherwise, the centroid is used.
   *
   * @param g the geometry to find a point for
   * @return a point representing the Geometry
   */
  private static Coordinate getPoint(Geometry g) {
    if (g.getNumPoints() == 1)
      return g.getCoordinate();
    return g.getCentroid().getCoordinate();
  }

  /**
   * Gets the value for a point from the supplied attribute. The value is checked for validity, and
   * a default of 1 is used if necessary.
   *
   * @param feature the feature to extract the value from
   * @param attrExpr the expression specifying the attribute to read
   * @return the value for the point
   */
  private static double getPointValue(SimpleFeature feature, Expression attrExpr) {
    Double valObj = attrExpr.evaluate(feature, Double.class);
    if (valObj != null) {
      return valObj;
    }
    return 1;
  }
}
