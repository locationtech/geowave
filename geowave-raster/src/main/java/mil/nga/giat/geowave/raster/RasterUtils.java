package mil.nga.giat.geowave.raster;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.geom.AffineTransform;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import javax.media.jai.BorderExtender;
import javax.media.jai.Histogram;
import javax.media.jai.Interpolation;
import javax.media.jai.JAI;
import javax.media.jai.PlanarImage;
import javax.media.jai.RenderedImageAdapter;
import javax.media.jai.RenderedOp;
import javax.media.jai.TiledImage;
import javax.media.jai.operator.ScaleDescriptor;

import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.raster.plugin.GeoWaveGTRasterFormat;

import org.apache.log4j.Logger;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.builder.GridToEnvelopeMapper;
import org.geotools.referencing.operation.matrix.MatrixFactory;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.resources.i18n.ErrorKeys;
import org.geotools.resources.i18n.Errors;
import org.geotools.resources.image.ImageUtilities;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.geometry.Envelope;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.Matrix;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier;

public class RasterUtils
{
	private final static Logger LOGGER = Logger.getLogger(RasterUtils.class);
	private static final int MIN_SEGMENTS = 5;
	private static final int MAX_SEGMENTS = 500;
	private static final double SIMPLIFICATION_MAX_DEGREES = 0.0001;
	private static int DEFAULT_IMAGE_TYPE = BufferedImage.TYPE_3BYTE_BGR;

	public static Geometry getFootprint(
			final ReferencedEnvelope projectedReferenceEnvelope,
			final GridCoverage gridCoverage ) {
		try {

			final Envelope sampleEnvelope = gridCoverage.getEnvelope();
			final double avgSpan = (projectedReferenceEnvelope.getSpan(0) + projectedReferenceEnvelope.getSpan(1)) / 2;

			final Coordinate[] polyCoords = getWorldCoordinates(
					sampleEnvelope.getMinimum(0),
					sampleEnvelope.getMinimum(1),
					sampleEnvelope.getMaximum(0),
					sampleEnvelope.getMaximum(1),
					(int) Math.min(
							Math.max(
									(avgSpan * MIN_SEGMENTS) / SIMPLIFICATION_MAX_DEGREES,
									MIN_SEGMENTS),
							MAX_SEGMENTS),
					CRS.findMathTransform(
							gridCoverage.getCoordinateReferenceSystem(),
							GeoWaveGTRasterFormat.DEFAULT_CRS,
							true));
			return DouglasPeuckerSimplifier.simplify(
					new GeometryFactory().createPolygon(polyCoords),
					SIMPLIFICATION_MAX_DEGREES);
		}
		catch (MismatchedDimensionException | TransformException | FactoryException e1) {
			LOGGER.warn(
					"Unable to calculate grid coverage footprint",
					e1);
		}
		return null;
	}

	public static Geometry combineIntoOneGeometry(
			final Geometry geometry1,
			final Geometry geometry2 ) {
		if (geometry1 == null) {
			return geometry2;
		}
		else if (geometry2 == null) {
			return geometry1;
		}
		final List<Geometry> geometry = new ArrayList<Geometry>();
		geometry.add(geometry1);
		geometry.add(geometry2);
		return DouglasPeuckerSimplifier.simplify(
				combineIntoOneGeometry(geometry),
				SIMPLIFICATION_MAX_DEGREES);
	}

	private static Geometry combineIntoOneGeometry(
			final Collection<Geometry> geometries ) {
		final GeometryFactory factory = JTSFactoryFinder.getGeometryFactory(null);

		// note the following geometry collection may be invalid (say with
		// overlapping polygons)
		final Geometry geometryCollection = factory.buildGeometry(geometries);

		return geometryCollection.union();
	}

	private static Coordinate[] getWorldCoordinates(
			final double minX,
			final double minY,
			final double maxX,
			final double maxY,
			final int numPointsPerSegment,
			final MathTransform gridToCRS )
			throws MismatchedDimensionException,
			TransformException {
		final Point2D[] screenCoordinates = getScreenCoordinates(
				minX,
				minY,
				maxX,
				maxY,
				numPointsPerSegment);
		final Coordinate[] worldCoordinates = new Coordinate[screenCoordinates.length];
		for (int i = 0; i < screenCoordinates.length; i++) {
			final DirectPosition2D worldPt = new DirectPosition2D();
			final DirectPosition2D dp = new DirectPosition2D(
					screenCoordinates[i]);
			gridToCRS.transform(
					dp,
					worldPt);
			worldCoordinates[i] = new Coordinate(
					worldPt.getX(),
					worldPt.getY());
		}
		return worldCoordinates;
	}

	private static Point2D[] getScreenCoordinates(
			final double minX,
			final double minY,
			final double maxX,
			final double maxY,
			final int numPointsPerSegment ) {
		final Point2D[] coordinates = new Point2D[((numPointsPerSegment - 1) * 4) + 1];
		fillCoordinates(
				true,
				minX,
				minY,
				maxY,
				(maxY - minY) / (numPointsPerSegment - 1),
				0,
				coordinates);
		fillCoordinates(
				false,
				maxY,
				minX,
				maxX,
				(maxX - minX) / (numPointsPerSegment - 1),
				numPointsPerSegment - 1,
				coordinates);
		fillCoordinates(
				true,
				maxX,
				maxY,
				minY,
				(maxY - minY) / (numPointsPerSegment - 1),
				(numPointsPerSegment - 1) * 2,
				coordinates);
		fillCoordinates(
				false,
				minY,
				maxX,
				minX,
				(maxX - minX) / (numPointsPerSegment - 1),
				(numPointsPerSegment - 1) * 3,
				coordinates);
		return coordinates;
	}

	private static void fillCoordinates(
			final boolean constantX,
			final double constant,
			final double start,
			final double stop,
			final double inc,
			final int coordinateArrayOffset,
			final Point2D[] coordinates ) {
		int i = coordinateArrayOffset;

		if (constantX) {
			final double x = constant;
			if (stop < start) {
				for (double y = start; y >= stop; y -= inc) {
					coordinates[i++] = new Point2D.Double(
							x,
							y);
				}
			}
			else {
				for (double y = start; y <= stop; y += inc) {
					coordinates[i++] = new Point2D.Double(
							x,
							y);
				}
			}
		}
		else {
			final double y = constant;
			if (stop < start) {
				double x = start;
				while (x >= stop) {
					coordinates[i] = new Point2D.Double(
							x,
							y);
					i++;
					x = start - ((i - coordinateArrayOffset) * inc);
				}
			}
			else {
				for (double x = start; x <= stop; x += inc) {
					coordinates[i++] = new Point2D.Double(
							x,
							y);
				}
			}
		}
	}

	/**
	 * Creates a math transform using the information provided.
	 *
	 * @return The math transform.
	 * @throws IllegalStateException
	 *             if the grid range or the envelope were not set.
	 */
	public static MathTransform createTransform(
			final double[] idRangePerDimension,
			final MultiDimensionalNumericData fullBounds )
			throws IllegalStateException {
		final GridToEnvelopeMapper mapper = new GridToEnvelopeMapper();
		final boolean swapXY = mapper.getSwapXY();
		final boolean[] reverse = mapper.getReverseAxis();
		final PixelInCell gridType = PixelInCell.CELL_CORNER;
		final int dimension = 2;
		/*
		 * Setup the multi-dimensional affine transform for use with OpenGIS.
		 * According OpenGIS specification, transforms must map pixel center.
		 * This is done by adding 0.5 to grid coordinates.
		 */
		final double translate;
		if (PixelInCell.CELL_CENTER.equals(gridType)) {
			translate = 0.5;
		}
		else if (PixelInCell.CELL_CORNER.equals(gridType)) {
			translate = 0.0;
		}
		else {
			throw new IllegalStateException(
					Errors.format(
							ErrorKeys.ILLEGAL_ARGUMENT_$2,
							"gridType",
							gridType));
		}
		final Matrix matrix = MatrixFactory.create(dimension + 1);
		final double[] minValuesPerDimension = fullBounds.getMinValuesPerDimension();
		final double[] maxValuesPerDimension = fullBounds.getMaxValuesPerDimension();
		for (int i = 0; i < dimension; i++) {
			// NOTE: i is a dimension in the 'gridRange' space (source
			// coordinates).
			// j is a dimension in the 'userRange' space (target coordinates).
			int j = i;
			if (swapXY && (j <= 1)) {
				j = 1 - j;
			}
			double scale = idRangePerDimension[j];
			double offset;
			if ((reverse == null) || (j >= reverse.length) || !reverse[j]) {
				offset = minValuesPerDimension[j];
			}
			else {
				scale = -scale;
				offset = maxValuesPerDimension[j];
			}
			offset -= scale * (-translate);
			matrix.setElement(
					j,
					j,
					0.0);
			matrix.setElement(
					j,
					i,
					scale);
			matrix.setElement(
					j,
					dimension,
					offset);
		}
		return ProjectiveTransform.create(matrix);
	}

	/**
	 * Returns the math transform as a two-dimensional affine transform.
	 *
	 * @return The math transform as a two-dimensional affine transform.
	 * @throws IllegalStateException
	 *             if the math transform is not of the appropriate type.
	 */
	public static AffineTransform createAffineTransform(
			final double[] idRangePerDimension,
			final MultiDimensionalNumericData fullBounds )
			throws IllegalStateException {
		final MathTransform transform = createTransform(
				idRangePerDimension,
				fullBounds);
		if (transform instanceof AffineTransform) {
			return (AffineTransform) transform;
		}
		throw new IllegalStateException(
				Errors.format(ErrorKeys.NOT_AN_AFFINE_TRANSFORM));
	}

	public static void fillWithNoDataValues(
			final WritableRaster raster,
			final double[][] noDataValues ) {
		if ((noDataValues != null) && (noDataValues.length >= raster.getNumBands())) {
			final double[] noDataFilledArray = new double[raster.getWidth() * raster.getHeight()];
			for (int b = 0; b < raster.getNumBands(); b++) {
				if ((noDataValues[b] != null) && (noDataValues[b].length > 0)) {
					// just fill every sample in this band with the first no
					// data value for that band
					Arrays.fill(
							noDataFilledArray,
							noDataValues[b][0]);
					raster.setSamples(
							raster.getMinX(),
							raster.getMinY(),
							raster.getWidth(),
							raster.getHeight(),
							b,
							noDataFilledArray);
				}
			}
		}
	}

	public static GridCoverage2D mosaicGridCoverages(
			final Iterator<GridCoverage> gridCoverages,
			final Color backgroundColor,
			final Color outputTransparentColor,
			final Rectangle pixelDimension,
			final GeneralEnvelope requestEnvelope,
			final double levelResX,
			final double levelResY,
			final double[][] noDataValues,
			final boolean xAxisSwitch,
			final GridCoverageFactory coverageFactory,
			final String coverageName,
			final Integer interpolation,
			final Histogram histogram ) {
		final double rescaleX = levelResX / (requestEnvelope.getSpan(0) / pixelDimension.getWidth());
		final double rescaleY = levelResY / (requestEnvelope.getSpan(1) / pixelDimension.getHeight());
		final double width = pixelDimension.getWidth() / rescaleX;
		final double height = pixelDimension.getHeight() / rescaleY;

		final int imageWidth = (int) Math.max(
				Math.round(width),
				1);
		final int imageHeight = (int) Math.max(
				Math.round(height),
				1);

		BufferedImage image = null;

		while (gridCoverages.hasNext()) {
			final GridCoverage currentCoverage = gridCoverages.next();
			final Envelope coverageEnv = currentCoverage.getEnvelope();
			final RenderedImage coverageImage = currentCoverage.getRenderedImage();
			if (image == null) {
				image = copyImage(
						imageWidth,
						imageHeight,
						backgroundColor,
						noDataValues,
						coverageImage);
			}
			final int posx = (int) ((coverageEnv.getMinimum(0) - requestEnvelope.getMinimum(0)) / levelResX);
			final int posy = (int) ((requestEnvelope.getMaximum(1) - coverageEnv.getMaximum(1)) / levelResY);

			image.getRaster().setRect(
					posx,
					posy,
					coverageImage.getData());

		}

		if (image == null) {
			image = getEmptyImage(
					imageWidth,
					imageHeight,
					backgroundColor,
					null);// the transparent color will be used later
		}

		GeneralEnvelope resultEnvelope = null;

		if (xAxisSwitch) {
			final Rectangle2D tmp = new Rectangle2D.Double(
					requestEnvelope.getMinimum(1),
					requestEnvelope.getMinimum(0),
					requestEnvelope.getSpan(1),
					requestEnvelope.getSpan(0));
			resultEnvelope = new GeneralEnvelope(
					tmp);
			resultEnvelope.setCoordinateReferenceSystem(requestEnvelope.getCoordinateReferenceSystem());
		}
		else {
			resultEnvelope = requestEnvelope;
		}

		image = toBufferedImage(
				rescaleImageViaPlanarImage(
						interpolation,
						rescaleX,
						rescaleY,
						image),
				image.getType());
		RenderedImage result;
		if (outputTransparentColor == null) {
			result = image;
		}
		else {
			result = ImageUtilities.maskColor(
					outputTransparentColor,
					image);
		}
		if (histogram != null) {
			// we should perform histogram equalization
			final int numBands = histogram.getNumBands();
			final float[][] cdFeq = new float[numBands][];
			for (int b = 0; b < numBands; b++) {
				final int numBins = histogram.getNumBins()[b];
				cdFeq[b] = new float[numBins];
				for (int i = 0; i < numBins; i++) {
					cdFeq[b][i] = (float) (i + 1) / (float) (numBins);
				}
			}
			final RenderedImageAdapter adaptedResult = new RenderedImageAdapter(
					result);
			adaptedResult.setProperty(
					"histogram",
					histogram);
			result = JAI.create(
					"matchcdf",
					adaptedResult,
					cdFeq);
		}
		return coverageFactory.create(
				coverageName,
				result,
				resultEnvelope);
	}

	public static BufferedImage toBufferedImage(
			final Image image,
			final int type ) {
		final BufferedImage bi = new BufferedImage(
				image.getWidth(null),
				image.getHeight(null),
				type);
		final Graphics g = bi.getGraphics();
		g.drawImage(
				image,
				0,
				0,
				null);
		g.dispose();
		return bi;
	}

	private static BufferedImage copyImage(
			final int targetWidth,
			final int targetHeight,
			final Color backgroundColor,
			final double[][] noDataValues,
			final RenderedImage originalImage ) {
		Hashtable<String, Object> properties = null;

		if (originalImage.getPropertyNames() != null) {
			properties = new Hashtable<String, Object>();
			for (final String name : originalImage.getPropertyNames()) {
				properties.put(
						name,
						originalImage.getProperty(name));
			}
		}

		final SampleModel sm = originalImage.getSampleModel().createCompatibleSampleModel(
				targetWidth,
				targetHeight);
		final WritableRaster raster = Raster.createWritableRaster(
				sm,
				null);

		final ColorModel colorModel = originalImage.getColorModel();
		final boolean alphaPremultiplied = colorModel.isAlphaPremultiplied();

		RasterUtils.fillWithNoDataValues(
				raster,
				noDataValues);
		final BufferedImage image = new BufferedImage(
				colorModel,
				raster,
				alphaPremultiplied,
				properties);
		if (noDataValues == null) {
			final Graphics2D g2D = (Graphics2D) image.getGraphics();
			final Color save = g2D.getColor();
			g2D.setColor(backgroundColor);
			g2D.fillRect(
					0,
					0,
					image.getWidth(),
					image.getHeight());
			g2D.setColor(save);
		}
		return image;
	}

	private static Image rescaleImageViaPlanarImage(
			Integer interpolation,
			final double rescaleX,
			final double rescaleY,
			final BufferedImage image ) {
		final PlanarImage planarImage = new TiledImage(
				image,
				image.getWidth(),
				image.getHeight());

		interpolation = Interpolation.INTERP_BICUBIC;
		if (interpolation != null) {
			if (interpolation.intValue() == 1) {
				interpolation = Interpolation.INTERP_NEAREST;
			}

			else if (interpolation.intValue() == 2) {
				interpolation = Interpolation.INTERP_BILINEAR;
			}
		}
		final RenderingHints scalingHints = new RenderingHints(
				RenderingHints.KEY_RENDERING,
				RenderingHints.VALUE_RENDER_QUALITY);
		scalingHints.put(
				RenderingHints.KEY_ALPHA_INTERPOLATION,
				RenderingHints.VALUE_ALPHA_INTERPOLATION_QUALITY);
		scalingHints.put(
				RenderingHints.KEY_ANTIALIASING,
				RenderingHints.VALUE_ANTIALIAS_ON);
		scalingHints.put(
				RenderingHints.KEY_COLOR_RENDERING,
				RenderingHints.VALUE_COLOR_RENDER_QUALITY);
		scalingHints.put(
				RenderingHints.KEY_DITHERING,
				RenderingHints.VALUE_DITHER_ENABLE);
		scalingHints.put(
				JAI.KEY_BORDER_EXTENDER,
				BorderExtender.createInstance(BorderExtender.BORDER_COPY));
		final RenderedOp result = ScaleDescriptor.create(
				planarImage,
				new Float(
						rescaleX),
				new Float(
						rescaleY),
				0.0f,
				0.0f,
				Interpolation.getInstance(interpolation),
				scalingHints);

		final WritableRaster scaledImageRaster = (WritableRaster) result.getData();

		final ColorModel colorModel = image.getColorModel();

		final BufferedImage scaledImage = new BufferedImage(
				colorModel,
				scaledImageRaster,
				image.isAlphaPremultiplied(),
				null);
		return scaledImage;
	}

	public static BufferedImage getEmptyImage(
			final int width,
			final int height,
			final Color backgroundColor,
			final Color outputTransparentColor ) {
		BufferedImage emptyImage = new BufferedImage(
				width,
				height,
				DEFAULT_IMAGE_TYPE);

		final Graphics2D g2D = (Graphics2D) emptyImage.getGraphics();
		final Color save = g2D.getColor();
		g2D.setColor(backgroundColor);
		g2D.fillRect(
				0,
				0,
				emptyImage.getWidth(),
				emptyImage.getHeight());
		g2D.setColor(save);

		if (outputTransparentColor != null) {
			emptyImage = new RenderedImageAdapter(
					ImageUtilities.maskColor(
							outputTransparentColor,
							emptyImage)).getAsBufferedImage();
		}
		return emptyImage;
	}

}
