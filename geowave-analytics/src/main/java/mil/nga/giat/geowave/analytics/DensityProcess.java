package mil.nga.giat.geowave.analytics;

import mil.nga.giat.geowave.vector.plugin.DecimationProcess;
import mil.nga.giat.geowave.vector.plugin.GeoWaveFeatureCollection;
import mil.nga.giat.geowave.vector.plugin.GeoWaveGTDataStore;

import org.apache.log4j.Logger;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.Hints;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.geotools.referencing.CRS;
import org.opengis.coverage.grid.GridGeometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.expression.Expression;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.opengis.util.ProgressListener;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

public class DensityProcess implements
		GSProcess
{
	private final static Logger LOGGER = Logger.getLogger(DensityProcess.class);

	@DescribeResult(name = "result", description = "The heat map surface as a raster")
	public GridCoverage2D execute(

			// process data
			@DescribeParameter(name = "data", description = "Features containing the data points")
			final SimpleFeatureCollection obsFeatures,

			@DescribeParameter(name = "minLevel", description = "The min level to use from the stats table", defaultValue = "1")
			final Integer minLevel,
			@DescribeParameter(name = "maxLevel", description = "The max level to use from the stats table")
			final Integer maxLevel,
			@DescribeParameter(name = "statsName", description = "The name given to the stats entries when inserted into the stats table", defaultValue = "")
			final String statsName,
			@DescribeParameter(name = "weightAttr", description = "Featuretype attribute containing the point weight value", min = 0, max = 1)
			final String valueAttr,

			// output image parameters
			@DescribeParameter(name = "outputBBOX", description = "Georeferenced bounding box of the output")
			final ReferencedEnvelope argOutputEnv,
			@DescribeParameter(name = "outputWidth", description = "Width of the output raster")
			final Integer argOutputWidth,
			@DescribeParameter(name = "outputHeight", description = "Height of the output raster")
			final Integer argOutputHeight,

			final ProgressListener monitor )
			throws ProcessException {

		/**
		 * -------- Extract required information from process arguments
		 * -------------
		 */
		final int outputWidth = argOutputWidth;
		final int outputHeight = argOutputHeight;
		final int gridWidth = outputWidth;
		final int gridHeight = outputHeight;
		/**
		 * Compute transform to convert input coords into output CRS
		 */
		final CoordinateReferenceSystem srcCRS = obsFeatures.getSchema().getCoordinateReferenceSystem();
		final CoordinateReferenceSystem dstCRS = argOutputEnv.getCoordinateReferenceSystem();
		MathTransform trans = null;
		try {
			trans = CRS.findMathTransform(
					srcCRS,
					dstCRS);
		}
		catch (final FactoryException e) {
			throw new ProcessException(
					e);
		}

		/**
		 * -------------- Extract the input observation points -----------
		 */
		final HeatmapSurface heatMap = new HeatmapSurface(
				0,
				argOutputEnv,
				gridWidth,
				gridHeight);
		try {
			extractPoints(
					obsFeatures,
					valueAttr,
					trans,
					heatMap);
		}
		catch (final CQLException e) {
			throw new ProcessException(
					e);
		}

		/**
		 * --------------- Do the processing ------------------------------
		 */
		// Stopwatch sw = new Stopwatch();
		// compute the heatmap at the specified resolution
		float[][] heatMapGrid = heatMap.computeSurface();

		// flip now, since grid size may be smaller
		heatMapGrid = flipXY(heatMapGrid);

		// convert to the GridCoverage2D required for output
		final GridCoverageFactory gcf = CoverageFactoryFinder.getGridCoverageFactory(null);
		final GridCoverage2D gridCov = gcf.create(
				"Process Results",
				heatMapGrid,
				argOutputEnv);

		return gridCov;
	}

	/**
	 * Flips an XY matrix along the X=Y axis, and inverts the Y axis. Used to
	 * convert from "map orientation" into the "image orientation" used by
	 * GridCoverageFactory. The surface interpolation is done on an XY grid,
	 * with Y=0 being the bottom of the space. GridCoverages are stored in an
	 * image format, in a YX grid with Y=0 being the top.
	 * 
	 * @param grid
	 *            the grid to flip
	 * @return the flipped grid
	 */
	private float[][] flipXY(
			final float[][] grid ) {
		final int xsize = grid.length;
		final int ysize = grid[0].length;

		final float[][] grid2 = new float[ysize][xsize];
		for (int ix = 0; ix < xsize; ix++) {
			for (int iy = 0; iy < ysize; iy++) {
				final int iy2 = ysize - iy - 1;
				grid2[iy2][ix] = grid[ix][iy];
			}
		}
		return grid2;
	}

	/**
	 * Given a target query and a target grid geometry returns the query to be
	 * used to read the input data of the process involved in rendering. In this
	 * process this method is used to:
	 * <ul>
	 * <li>determine the extent & CRS of the output grid
	 * <li>expand the query envelope to ensure stable surface generation
	 * <li>modify the query hints to ensure point features are returned
	 * </ul>
	 * Note that in order to pass validation, all parameters named here must
	 * also appear in the parameter list of the <tt>execute</tt> method, even if
	 * they are not used there.
	 * 
	 * @param argRadiusPixels
	 *            the feature type attribute that contains the observed surface
	 *            value
	 * @param targetQuery
	 *            the query used against the data source
	 * @param targetGridGeometry
	 *            the grid geometry of the destination image
	 * @return The transformed query
	 */
	public Query invertQuery(
			@DescribeParameter(name = "minLevel", description = "The min level to use from the stats table", defaultValue = "1")
			final Integer minLevel,
			@DescribeParameter(name = "maxLevel", description = "The max level to use from the stats table")
			final Integer maxLevel,
			@DescribeParameter(name = "statsName", description = "The name given to the stats entries when inserted into the stats table", defaultValue = "")
			final String statsName,
			// output image parameters
			@DescribeParameter(name = "outputBBOX", description = "Georeferenced bounding box of the output")
			final ReferencedEnvelope argOutputEnv,
			@DescribeParameter(name = "outputWidth", description = "Width of the output raster")
			final Integer argOutputWidth,
			@DescribeParameter(name = "outputHeight", description = "Height of the output raster")
			final Integer argOutputHeight,

			final Query targetQuery,
			final GridGeometry targetGridGeometry )
			throws ProcessException {

		// TODO: handle different CRSes in input and output

		// clear properties to force all attributes to be read
		// (required because the SLD processor cannot see the value attribute
		// specified in the
		// transformation)
		// TODO: set the properties to read only the specified value attribute
		targetQuery.setProperties(Query.ALL_PROPERTIES);

		// set the decimation hint to ensure points are read
		final Hints hints = targetQuery.getHints();
		hints.put(
				Hints.GEOMETRY_DISTANCE,
				0.0);
		double pixelSizeDefaultCrs;
		try {
			pixelSizeDefaultCrs = pixelSize(
					argOutputEnv.transform(
							GeoWaveGTDataStore.DEFAULT_CRS,
							true),
					argOutputWidth,
					argOutputHeight);
			final double log2PixelSize = Math.log(pixelSizeDefaultCrs * 180.0) / Math.log(2);

			int level = (int) Math.round(log2PixelSize);
			if (level < minLevel) {
				level = minLevel;
			}
			else if (level >= maxLevel) {
				level = maxLevel;
			}
			hints.put(
					GeoWaveFeatureCollection.LEVEL,
					level);
			hints.put(
					GeoWaveFeatureCollection.STATS_NAME,
					statsName);
			hints.put(
					DecimationProcess.OUTPUT_BBOX,
					argOutputEnv);
		}
		catch (TransformException | FactoryException e) {
			LOGGER.warn(
					"Unable to transform to default CRS",
					e);
		}
		return targetQuery;
	}

	private double pixelSize(
			final ReferencedEnvelope outputEnv,
			final int outputWidth,
			final int outputHeight ) {
		// error-proofing
		if (outputEnv.getWidth() <= 0) {
			return 0;
		}
		// assume view is isotropic
		return outputWidth / outputEnv.getWidth();
	}

	public static void extractPoints(
			final SimpleFeatureCollection obsPoints,
			final String attrName,
			final MathTransform trans,
			final HeatmapSurface heatMap )
			throws CQLException {
		Expression attrExpr = null;
		if (attrName != null) {
			attrExpr = ECQL.toExpression(attrName);
		}

		final SimpleFeatureIterator obsIt = obsPoints.features();

		final double[] srcPt1 = new double[2];
		final double[] srcPt2 = new double[2];
		final double[] dstPt1 = new double[2];
		final double[] dstPt2 = new double[2];
		try {
			while (obsIt.hasNext()) {
				final SimpleFeature feature = obsIt.next();

				try {
					// get the weight value, if any
					double val = 1;
					if (attrExpr != null) {
						val = getPointValue(
								feature,
								attrExpr);
					}

					// get the point location from the geometry
					final Geometry geom = (Geometry) feature.getDefaultGeometry();
					final Coordinate[] p = getBBOX(geom);
					if (p.length == 1) {
						srcPt1[0] = p[0].x;
						srcPt1[1] = p[0].y;
						trans.transform(
								srcPt1,
								0,
								dstPt1,
								0,
								1);

						heatMap.addPoint(
								dstPt1[0],
								dstPt1[1],
								val);
					}
					else {
						srcPt1[0] = p[0].x;
						srcPt1[1] = p[0].y;
						trans.transform(
								srcPt1,
								0,
								dstPt1,
								0,
								1);

						srcPt2[0] = p[1].x;
						srcPt2[1] = p[1].y;

						trans.transform(
								srcPt2,
								0,
								dstPt2,
								0,
								1);
						heatMap.addPoints(
								dstPt1[0],
								dstPt1[1],
								dstPt2[0],
								dstPt2[1],
								val);
					}
				}
				catch (final Exception e) {
					// just carry on for now (debugging)
					// throw new ProcessException("Expression " + attrExpr +
					// " failed to evaluate to a numeric value", e);
				}
			}
		}
		finally {
			obsIt.close();
		}
	}

	/**
	 * Gets a point to represent the Geometry. If the Geometry is a point, this
	 * is returned. Otherwise, the centroid is used.
	 * 
	 * @param g
	 *            the geometry to find a point for
	 * @return a point representing the Geometry
	 */
	private static Coordinate[] getBBOX(
			final Geometry g ) {
		if (g.getNumPoints() == 1) {
			return new Coordinate[] {
				g.getCoordinate()
			};
		}
		final Envelope e = g.getEnvelopeInternal();
		return new Coordinate[] {
			new Coordinate(
					e.getMinX(),
					e.getMinY()),
			new Coordinate(
					e.getMaxX(),
					e.getMaxY())
		};
	}

	/**
	 * Gets the value for a point from the supplied attribute. The value is
	 * checked for validity, and a default of 1 is used if necessary.
	 * 
	 * @param feature
	 *            the feature to extract the value from
	 * @param attrExpr
	 *            the expression specifying the attribute to read
	 * @return the value for the point
	 */
	private static double getPointValue(
			final SimpleFeature feature,
			final Expression attrExpr ) {
		final Double valObj = attrExpr.evaluate(
				feature,
				Double.class);
		if (valObj != null) {
			return valObj;
		}
		return 1;
	}
}
