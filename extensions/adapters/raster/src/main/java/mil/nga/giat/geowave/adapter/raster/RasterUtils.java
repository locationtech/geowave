package mil.nga.giat.geowave.adapter.raster;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.Transparency;
import java.awt.geom.AffineTransform;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.IndexColorModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import javax.media.jai.BorderExtender;
import javax.media.jai.Histogram;
import javax.media.jai.Interpolation;
import javax.media.jai.JAI;
import javax.media.jai.PlanarImage;
import javax.media.jai.RasterFactory;
import javax.media.jai.RenderedImageAdapter;
import javax.media.jai.RenderedOp;
import javax.media.jai.TiledImage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.geotools.coverage.Category;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.TypeMap;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.processing.Operations;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.image.ImageWorker;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.builder.GridToEnvelopeMapper;
import org.geotools.referencing.operation.matrix.MatrixFactory;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.resources.i18n.ErrorKeys;
import org.geotools.resources.i18n.Errors;
import org.geotools.resources.image.ImageUtilities;
import org.geotools.util.NumberRange;
import org.opengis.coverage.SampleDimension;
import org.opengis.coverage.SampleDimensionType;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.geometry.Envelope;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.Matrix;
import org.opengis.referencing.operation.TransformException;

import com.google.common.collect.ImmutableMap;
import com.sun.media.imageioimpl.common.BogusColorSpace;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.RasterTileMergeStrategy;
import mil.nga.giat.geowave.adapter.raster.plugin.GeoWaveGTRasterFormat;
import mil.nga.giat.geowave.core.index.FloatCompareUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

public class RasterUtils
{
	private static final RenderingHints DEFAULT_RENDERING_HINTS = new RenderingHints(
			new ImmutableMap.Builder().put(
					RenderingHints.KEY_RENDERING,
					RenderingHints.VALUE_RENDER_QUALITY).put(
					RenderingHints.KEY_ALPHA_INTERPOLATION,
					RenderingHints.VALUE_ALPHA_INTERPOLATION_QUALITY).put(
					RenderingHints.KEY_ANTIALIASING,
					RenderingHints.VALUE_ANTIALIAS_ON).put(
					RenderingHints.KEY_COLOR_RENDERING,
					RenderingHints.VALUE_COLOR_RENDER_QUALITY).put(
					RenderingHints.KEY_DITHERING,
					RenderingHints.VALUE_DITHER_ENABLE).put(
					JAI.KEY_BORDER_EXTENDER,
					BorderExtender.createInstance(BorderExtender.BORDER_COPY)).build());
	private static Operations resampleOperations;
	private final static Logger LOGGER = LoggerFactory.getLogger(RasterUtils.class);
	private static final int MIN_SEGMENTS = 5;
	private static final int MAX_SEGMENTS = 500;

	private static final int MAX_VERTICES_BEFORE_SIMPLIFICATION = 20;
	private static final double SIMPLIFICATION_MAX_DEGREES = 0.0001;

	public static Geometry getFootprint(
			final GridCoverage gridCoverage,
			final CoordinateReferenceSystem targetCrs ) {
		return getFootprint(
				getReferenceEnvelope(
						gridCoverage,
						targetCrs),
				gridCoverage);
	}

	public static ReferencedEnvelope getReferenceEnvelope(
			final GridCoverage gridCoverage,
			final CoordinateReferenceSystem targetCrs ) {
		final CoordinateReferenceSystem sourceCrs = gridCoverage.getCoordinateReferenceSystem();
		final Envelope sampleEnvelope = gridCoverage.getEnvelope();

		final ReferencedEnvelope sampleReferencedEnvelope = new ReferencedEnvelope(
				new com.vividsolutions.jts.geom.Envelope(
						sampleEnvelope.getMinimum(0),
						sampleEnvelope.getMaximum(0),
						sampleEnvelope.getMinimum(1),
						sampleEnvelope.getMaximum(1)),
				gridCoverage.getCoordinateReferenceSystem());

		ReferencedEnvelope projectedReferenceEnvelope = sampleReferencedEnvelope;
		if ((targetCrs != null) && !targetCrs.equals(sourceCrs)) {
			try {
				projectedReferenceEnvelope = sampleReferencedEnvelope.transform(
						targetCrs,
						true);
			}
			catch (TransformException | FactoryException e) {
				LOGGER.warn(
						"Unable to transform envelope of grid coverage to " + targetCrs.getName(),
						e);
			}
		}
		return projectedReferenceEnvelope;
	}

	public static Geometry getFootprint(
			final ReferencedEnvelope projectedReferenceEnvelope,
			final GridCoverage gridCoverage ) {
		try {
			final Envelope sampleEnvelope = gridCoverage.getEnvelope();
			final double avgSpan = (projectedReferenceEnvelope.getSpan(0) + projectedReferenceEnvelope.getSpan(1)) / 2;
			final MathTransform gridCrsToWorldCrs = CRS.findMathTransform(
					gridCoverage.getCoordinateReferenceSystem(),
					GeoWaveGTRasterFormat.DEFAULT_CRS,
					true);
			final Coordinate[] polyCoords = getWorldCoordinates(
					sampleEnvelope.getMinimum(0),
					sampleEnvelope.getMinimum(1),
					sampleEnvelope.getMaximum(0),
					sampleEnvelope.getMaximum(1),
					gridCrsToWorldCrs.isIdentity() ? 2 : (int) Math.min(
							Math.max(
									(avgSpan * MIN_SEGMENTS) / SIMPLIFICATION_MAX_DEGREES,
									MIN_SEGMENTS),
							MAX_SEGMENTS),
					gridCrsToWorldCrs);
			final Polygon poly = new GeometryFactory().createPolygon(polyCoords);
			if (polyCoords.length > MAX_VERTICES_BEFORE_SIMPLIFICATION) {
				final Geometry retVal = DouglasPeuckerSimplifier.simplify(
						poly,
						SIMPLIFICATION_MAX_DEGREES);
				if (retVal.isEmpty()) {
					return poly;
				}
				return retVal;
			}
			else {
				return poly;
			}
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
		// try {
		return geometryCollection.union();
		// }
		// catch (Exception e) {
		// LOGGER.warn("Error creating a union of this geometry collection", e);
		// return geometryCollection;
		// }
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
		final Point2D[] gridCoordinates = getGridCoordinates(
				minX,
				minY,
				maxX,
				maxY,
				numPointsPerSegment);
		final Coordinate[] worldCoordinates = new Coordinate[gridCoordinates.length];
		for (int i = 0; i < gridCoordinates.length; i++) {
			final DirectPosition2D worldPt = new DirectPosition2D();
			final DirectPosition2D dp = new DirectPosition2D(
					gridCoordinates[i]);
			gridToCRS.transform(
					dp,
					worldPt);
			worldCoordinates[i] = new Coordinate(
					worldPt.getX(),
					worldPt.getY());
		}
		return worldCoordinates;
	}

	private static Point2D[] getGridCoordinates(
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
			if (swapXY) {
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
			final Interpolation interpolation,
			final Histogram histogram,
			final boolean scaleTo8BitSet,
			final boolean scaleTo8Bit,
			final ColorModel defaultColorModel ) {

		if (pixelDimension == null) {
			LOGGER.error("Pixel dimension can not be null");
			throw new IllegalArgumentException(
					"Pixel dimension can not be null");
		}

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
		int numDimensions;
		SampleDimension[] sampleDimensions = null;
		double[][] extrema = null;
		boolean extremaValid = false;
		while (gridCoverages.hasNext()) {
			final GridCoverage currentCoverage = gridCoverages.next();
			if (sampleDimensions == null) {
				numDimensions = currentCoverage.getNumSampleDimensions();
				sampleDimensions = new SampleDimension[numDimensions];
				extrema = new double[2][numDimensions];
				extremaValid = true;
				for (int d = 0; d < numDimensions; d++) {
					sampleDimensions[d] = currentCoverage.getSampleDimension(d);
					extrema[0][d] = sampleDimensions[d].getMinimumValue();
					extrema[1][d] = sampleDimensions[d].getMaximumValue();
					if ((extrema[1][d] - extrema[0][d]) <= 0) {
						extremaValid = false;
					}
				}
			}

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
					null, // the transparent color will be used later
					defaultColorModel);
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
		final double scaleX = rescaleX * (width / imageWidth);
		final double scaleY = rescaleY * (height / imageHeight);
		if ((Math.abs(scaleX - 1) > FloatCompareUtils.COMP_EPSILON)
				|| (Math.abs(scaleY - 1) > FloatCompareUtils.COMP_EPSILON)) {
			image = rescaleImageViaPlanarImage(
					interpolation,
					rescaleX * (width / imageWidth),
					rescaleY * (height / imageHeight),
					image);

		}
		RenderedImage result = image;
		// hypothetically masking the output transparent color should happen
		// before histogram stretching, but the masking seems to only work now
		// when the image is bytes in each band which requires some amount of
		// modification to the original data, we'll use extrema
		if (extremaValid && scaleTo8Bit) {
			final int dataType = result.getData().getDataBuffer().getDataType();
			switch (dataType) {
			// in case the original image has a USHORT pixel type without
			// being associated
			// with an index color model I would still go to 8 bits
				case DataBuffer.TYPE_USHORT:
					if (result.getColorModel() instanceof IndexColorModel) {
						break;
					}
				case DataBuffer.TYPE_DOUBLE:
				case DataBuffer.TYPE_FLOAT:
					if (!scaleTo8BitSet && (dataType != DataBuffer.TYPE_USHORT)) {
						break;
					}
				case DataBuffer.TYPE_INT:
				case DataBuffer.TYPE_SHORT:
					// rescale to byte
					final ImageWorkerPredefineStats w = new ImageWorkerPredefineStats(
							result);
					// it was found that geoserver will perform this, and worse
					// perform it on local extrema calculated from a single
					// tile, this is our one opportunity to at least ensure this
					// transformation is done without too much harm by using
					// global extrema
					result = w.setExtrema(
							extrema).rescaleToBytes().getRenderedImage();
					break;
				default:
					// findbugs seems to want to have a default case, default is
					// to do nothing
					break;
			}
		}
		if (outputTransparentColor != null) {
			result = ImageUtilities.maskColor(
					outputTransparentColor,
					result);
		}
		if (histogram != null) {
			// we should perform histogram equalization
			final int numBands = histogram.getNumBands();
			final float[][] cdFeq = new float[numBands][];
			final double[][] computedExtrema = new double[2][numBands];
			for (int b = 0; b < numBands; b++) {
				computedExtrema[0][b] = histogram.getLowValue(b);
				computedExtrema[1][b] = histogram.getHighValue(b);
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
			adaptedResult.setProperty(
					"extrema",
					computedExtrema);
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

	@SuppressFBWarnings(value = {
		"RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"
	}, justification = "incorrect; drawImage has side effects")
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

	private static BufferedImage rescaleImageViaPlanarImage(
			final Interpolation interpolation,
			final double rescaleX,
			final double rescaleY,
			final BufferedImage image ) {
		final PlanarImage planarImage = new TiledImage(
				image,
				image.getWidth(),
				image.getHeight());
		final ImageWorker w = new ImageWorker(
				planarImage);
		w.scale(
				(float) rescaleX,
				(float) rescaleY,
				0.0f,
				0.0f,
				interpolation);
		final RenderedOp result = w.getRenderedOperation();
		final Raster raster = result.getData();
		final WritableRaster scaledImageRaster;
		if (raster instanceof WritableRaster) {
			scaledImageRaster = (WritableRaster) raster;
		}
		else {
			scaledImageRaster = raster.createCompatibleWritableRaster();
			scaledImageRaster.setDataElements(
					0,
					0,
					raster);
		}
		final ColorModel colorModel = image.getColorModel();
		try {
			final BufferedImage scaledImage = new BufferedImage(
					colorModel,
					scaledImageRaster,
					image.isAlphaPremultiplied(),
					null);

			return scaledImage;
		}
		catch (final IllegalArgumentException e) {
			LOGGER.warn(
					"Unable to rescale image",
					e);
			return image;
		}
	}

	public static void forceRenderingHints(
			final RenderingHints renderingHints ) {
		resampleOperations = new Operations(
				renderingHints);
	}

	public static synchronized Operations getCoverageOperations() {
		if (resampleOperations == null) {
			resampleOperations = new Operations(
					DEFAULT_RENDERING_HINTS);
		}
		return resampleOperations;
	}

	public static BufferedImage getEmptyImage(
			final int width,
			final int height,
			final Color backgroundColor,
			final Color outputTransparentColor,
			final ColorModel defaultColorModel ) {
		BufferedImage emptyImage = new BufferedImage(
				defaultColorModel,
				defaultColorModel.createCompatibleWritableRaster(
						width,
						height),
				defaultColorModel.isAlphaPremultiplied(),
				null);

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

	public static WritableRaster createRasterTypeDouble(
			final int numBands,
			final int tileSize ) {
		final WritableRaster raster = RasterFactory.createBandedRaster(
				DataBuffer.TYPE_DOUBLE,
				tileSize,
				tileSize,
				numBands,
				null);
		final double[] defaultValues = new double[tileSize * tileSize * numBands];
		Arrays.fill(
				defaultValues,
				Double.NaN);
		raster.setDataElements(
				0,
				0,
				tileSize,
				tileSize,
				defaultValues);
		return raster;
	}

	public static RasterDataAdapter createDataAdapterTypeDouble(
			final String coverageName,
			final int numBands,
			final int tileSize ) {
		return createDataAdapterTypeDouble(
				coverageName,
				numBands,
				tileSize,
				null);
	}

	public static RasterDataAdapter createDataAdapterTypeDouble(
			final String coverageName,
			final int numBands,
			final int tileSize,
			final RasterTileMergeStrategy<?> mergeStrategy ) {
		return createDataAdapterTypeDouble(
				coverageName,
				numBands,
				tileSize,
				null,
				null,
				null,
				mergeStrategy);
	}

	public static RasterDataAdapter createDataAdapterTypeDouble(
			final String coverageName,
			final int numBands,
			final int tileSize,
			final double[] minsPerBand,
			final double[] maxesPerBand,
			final String[] namesPerBand,
			final RasterTileMergeStrategy<?> mergeStrategy ) {
		final double[][] noDataValuesPerBand = new double[numBands][];
		final double[] backgroundValuesPerBand = new double[numBands];
		final int[] bitsPerSample = new int[numBands];
		for (int i = 0; i < numBands; i++) {
			noDataValuesPerBand[i] = new double[] {
				Double.valueOf(Double.NaN)
			};
			backgroundValuesPerBand[i] = Double.valueOf(Double.NaN);
			bitsPerSample[i] = DataBuffer.getDataTypeSize(DataBuffer.TYPE_DOUBLE);
		}
		final SampleModel sampleModel = createRasterTypeDouble(
				numBands,
				tileSize).getSampleModel();
		return new RasterDataAdapter(
				coverageName,
				sampleModel,
				new ComponentColorModel(
						new BogusColorSpace(
								numBands),
						bitsPerSample,
						false,
						false,
						Transparency.OPAQUE,
						DataBuffer.TYPE_DOUBLE),
				new HashMap<String, String>(),
				tileSize,
				minsPerBand,
				maxesPerBand,
				namesPerBand,
				noDataValuesPerBand,
				backgroundValuesPerBand,
				null,
				false,
				Interpolation.INTERP_NEAREST,
				false,
				mergeStrategy);
	}

	public static GridCoverage2D createCoverageTypeDouble(
			final String coverageName,
			final double westLon,
			final double eastLon,
			final double southLat,
			final double northLat,
			final WritableRaster raster ) {
		final GridCoverageFactory gcf = CoverageFactoryFinder.getGridCoverageFactory(null);
		Envelope mapExtent;
		try {
			mapExtent = new ReferencedEnvelope(
					westLon,
					eastLon,
					southLat,
					northLat,
					GeoWaveGTRasterFormat.DEFAULT_CRS);
		}
		catch (final IllegalArgumentException e) {
			LOGGER.warn(
					"Unable to use default CRS",
					e);
			mapExtent = new Envelope2D(
					new DirectPosition2D(
							westLon,
							southLat),
					new DirectPosition2D(
							eastLon,
							northLat));
		}
		return gcf.create(
				coverageName,
				raster,
				mapExtent);
	}

	public static GridCoverage2D createCoverageTypeDouble(
			final String coverageName,
			final double westLon,
			final double eastLon,
			final double southLat,
			final double northLat,
			final double[] minPerBand,
			final double[] maxPerBand,
			final String[] namePerBand,
			final WritableRaster raster ) {
		final GridCoverageFactory gcf = CoverageFactoryFinder.getGridCoverageFactory(null);
		Envelope mapExtent;
		try {
			mapExtent = new ReferencedEnvelope(
					westLon,
					eastLon,
					southLat,
					northLat,
					GeoWaveGTRasterFormat.DEFAULT_CRS);
		}
		catch (final IllegalArgumentException e) {
			LOGGER.warn(
					"Unable to use default CRS",
					e);
			mapExtent = new Envelope2D(
					new DirectPosition2D(
							westLon,
							southLat),
					new DirectPosition2D(
							eastLon,
							northLat));
		}
		final GridSampleDimension[] bands = new GridSampleDimension[raster.getNumBands()];
		create(
				namePerBand,
				raster.getSampleModel(),
				minPerBand,
				maxPerBand,
				bands);
		return gcf.create(
				coverageName,
				raster,
				mapExtent,
				bands);
	}

	/**
	 * NOTE: This is a small bit of functionality "inspired by"
	 * org.geotools.coverage.grid.RenderedSampleDimension ie. some of the code
	 * has been modified/simplified from the original version, but it had
	 * private visibility and could not be re-used as is Creates a set of sample
	 * dimensions for the data backing the given iterator. Particularly, it was
	 * desirable to be able to provide the name per band which was not provided
	 * in the original.
	 *
	 * @param name
	 *            The name for each band of the data (e.g. "Elevation").
	 * @param model
	 *            The image or raster sample model.
	 * @param min
	 *            The minimal value, or {@code null} for computing it
	 *            automatically.
	 * @param max
	 *            The maximal value, or {@code null} for computing it
	 *            automatically.
	 * @param dst
	 *            The array where to store sample dimensions. The array length
	 *            must matches the number of bands.
	 */
	private static void create(
			final CharSequence[] name,
			final SampleModel model,
			final double[] min,
			final double[] max,
			final GridSampleDimension[] dst ) {
		final int numBands = dst.length;
		if ((min != null) && (min.length != numBands)) {
			throw new IllegalArgumentException(
					Errors.format(
							ErrorKeys.NUMBER_OF_BANDS_MISMATCH_$3,
							numBands,
							min.length,
							"min[i]"));
		}
		if ((name != null) && (name.length != numBands)) {
			throw new IllegalArgumentException(
					Errors.format(
							ErrorKeys.NUMBER_OF_BANDS_MISMATCH_$3,
							numBands,
							name.length,
							"name[i]"));
		}
		if ((max != null) && (max.length != numBands)) {
			throw new IllegalArgumentException(
					Errors.format(
							ErrorKeys.NUMBER_OF_BANDS_MISMATCH_$3,
							numBands,
							max.length,
							"max[i]"));
		}
		/*
		 * Arguments are know to be valids. We now need to compute two ranges:
		 * 
		 * STEP 1: Range of target (sample) values. This is computed in the
		 * following block. STEP 2: Range of source (geophysics) values. It will
		 * be computed one block later.
		 * 
		 * The target (sample) values will typically range from 0 to 255 or 0 to
		 * 65535, but the general case is handled as well. If the source
		 * (geophysics) raster uses floating point numbers, then a "nodata"
		 * category may be added in order to handle NaN values. If the source
		 * raster use integer numbers instead, then we will rescale samples only
		 * if they would not fit in the target data type.
		 */
		final SampleDimensionType sourceType = TypeMap.getSampleDimensionType(
				model,
				0);
		final boolean sourceIsFloat = TypeMap.isFloatingPoint(sourceType);

		// Default to TYPE_BYTE for floating point images only; otherwise
		// keep unchanged.
		SampleDimensionType targetType = sourceIsFloat ? SampleDimensionType.UNSIGNED_8BITS : sourceType;

		// Default setting: no scaling
		final boolean targetIsFloat = TypeMap.isFloatingPoint(targetType);
		NumberRange targetRange = TypeMap.getRange(targetType);
		Category[] categories = new Category[1];
		final boolean needScaling;
		if (targetIsFloat) {
			// Never rescale if the target is floating point numbers.
			needScaling = false;
		}
		else if (sourceIsFloat) {
			// Always rescale for "float to integer" conversions. In addition,
			// Use 0 value as a "no data" category for unsigned data type only.
			needScaling = true;
			if (!TypeMap.isSigned(targetType)) {
				categories = new Category[2];
				categories[1] = Category.NODATA;
				targetRange = TypeMap.getPositiveRange(targetType);
			}
		}
		else {
			// In "integer to integer" conversions, rescale only if
			// the target range is smaller than the source range.
			needScaling = !targetRange.contains(TypeMap.getRange(sourceType));
		}

		/*
		 * Now, constructs the sample dimensions. We will inconditionnaly
		 * provides a "nodata" category for floating point images targeting
		 * unsigned integers, since we don't know if the user plan to have NaN
		 * values. Even if the current image doesn't have NaN values, it could
		 * have NaN later if the image uses a writable raster.
		 */
		for (int b = 0; b < numBands; b++) {
			// if (needScaling) {
			// sourceRange = NumberRange.create(
			// min[b],
			// max[b]).castTo(
			// sourceRange.getElementClass());
			// categories[0] = new Category(
			// name[b],
			// null,
			// targetRange,
			// sourceRange);
			// }
			// else {
			// categories[0] = new Category(
			// name[b],
			// null,
			// targetRange,
			// LinearTransform1D.IDENTITY);
			// }
			// dst[b] = new GridSampleDimension(
			// name[b],
			// categories,
			// null);
			categories[0] = new Category(
					name[b],
					(Color) null,
					targetRange);
			dst[b] = new GridSampleDimension(
					name[b],
					categories,
					null);
		}
	}
}
