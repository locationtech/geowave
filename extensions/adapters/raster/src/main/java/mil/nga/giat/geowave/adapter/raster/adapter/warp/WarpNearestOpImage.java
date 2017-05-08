package mil.nga.giat.geowave.adapter.raster.adapter.warp;

import it.geosolutions.jaiext.iterators.RandomIterFactory;
import it.geosolutions.jaiext.range.Range;

import java.awt.image.ColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.IndexColorModel;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.util.Map;

import javax.media.jai.ImageLayout;
import javax.media.jai.Interpolation;
import javax.media.jai.PlanarImage;
import javax.media.jai.ROI;
import javax.media.jai.RasterAccessor;
import javax.media.jai.Warp;
import javax.media.jai.iterator.RandomIter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This is code entirely intended to get around an issue on line 265 of
 * WarpOpImage in jai-ext. The following code does not work if the source is
 * significant lower resolution than the destination and seems unnecessary in
 * general:
 * 
 * roiTile = roi.intersect(new ROIShape(srcRectExpanded));
 * 
 * An <code>OpImage</code> implementing the general "Warp" operation as
 * described in <code>javax.media.jai.operator.WarpDescriptor</code>. It
 * supports the nearest-neighbor interpolation.
 * 
 * <p>
 * The layout for the destination image may be specified via the
 * <code>ImageLayout</code> parameter. However, only those settings suitable for
 * this operation will be used. The unsuitable settings will be replaced by
 * default suitable values. An optional ROI object and a NoData Range can be
 * used. If a backward mapped pixel lies outside ROI or it is a NoData, then the
 * destination pixel value is a background value.
 * 
 * @since EA2
 * @see javax.media.jai.Warp
 * @see javax.media.jai.WarpOpImage
 * @see javax.media.jai.operator.WarpDescriptor
 * @see WarpRIF
 * 
 */
@SuppressWarnings("unchecked")
@SuppressFBWarnings
final class WarpNearestOpImage extends
		WarpOpImage
{
	/** LookupTable used for a faster NoData check */
	private byte[][] byteLookupTable;

	/**
	 * Constructs a WarpNearestOpImage.
	 * 
	 * @param source
	 *            The source image.
	 * @param config
	 *            RenderingHints used in calculations.
	 * @param layout
	 *            The destination image layout.
	 * @param warp
	 *            An object defining the warp algorithm.
	 * @param interp
	 *            An object describing the interpolation method.
	 * @param roi
	 *            input ROI object used.
	 * @param noData
	 *            NoData Range object used for checking if NoData are present.
	 * 
	 */
	public WarpNearestOpImage(
			final RenderedImage source,
			final Map<?, ?> config,
			final ImageLayout layout,
			final Warp warp,
			final Interpolation interp,
			final ROI sourceROI,
			Range noData,
			double[] bkg ) {
		super(
				source,
				layout,
				config,
				false,
				null, // extender not needed in
						// nearest-neighbor
						// interpolation
				interp,
				warp,
				bkg,
				sourceROI,
				noData);

		/*
		 * If the source has IndexColorModel, override the default setting in
		 * OpImage. The dest shall have exactly the same SampleModel and
		 * ColorModel as the source. Note, in this case, the source should have
		 * an integral data type.
		 */
		final ColorModel srcColorModel = source.getColorModel();
		if (srcColorModel instanceof IndexColorModel) {
			sampleModel = source.getSampleModel().createCompatibleSampleModel(
					tileWidth,
					tileHeight);
			colorModel = srcColorModel;
		}

		/*
		 * Selection of a destinationNoData value for each datatype
		 */
		SampleModel sm = source.getSampleModel();
		// Source image data Type
		int srcDataType = sm.getDataType();

		// Creation of a lookuptable containing the values to use for no data
		if (srcDataType == DataBuffer.TYPE_BYTE && hasNoData) {
			int numBands = getNumBands();
			byteLookupTable = new byte[numBands][256];
			for (int b = 0; b < numBands; b++) {
				for (int i = 0; i < byteLookupTable[0].length; i++) {
					byte value = (byte) i;
					if (noDataRange.contains(value)) {
						byteLookupTable[b][i] = (byte) backgroundValues[b];
					}
					else {
						byteLookupTable[b][i] = value;
					}
				}
			}
		}
	}

	protected void computeRectByte(
			final PlanarImage src,
			final RasterAccessor dst,
			final RandomIter roiIter,
			boolean roiContainsTile ) {
		// Random Iterator on the source image bounds
		final RandomIter iter = RandomIterFactory.create(
				src,
				src.getBounds(),
				TILE_CACHED,
				ARRAY_CALC);
		// Initial settings
		final int minX = src.getMinX();
		final int maxX = src.getMaxX();
		final int minY = src.getMinY();
		final int maxY = src.getMaxY();

		final int dstWidth = dst.getWidth();
		final int dstHeight = dst.getHeight();
		final int dstBands = dst.getNumBands();

		final int lineStride = dst.getScanlineStride();
		final int pixelStride = dst.getPixelStride();
		final int[] bandOffsets = dst.getBandOffsets();
		final byte[][] data = dst.getByteDataArrays();

		final float[] warpData = new float[2 * dstWidth];

		int lineOffset = 0;

		// NO ROI AND NODATA
		if (caseA || (caseB && roiContainsTile)) {
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);
					// If the pixel is outside the input image bounds
					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (byte) backgroundValues[b];
							}
						}
					}
					else {
						// Nearest interpolation
						for (int b = 0; b < dstBands; b++) {
							data[b][pixelOffset + bandOffsets[b]] = (byte) (iter.getSample(
									sx,
									sy,
									b) & 0xFF);
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// ONLY ROI
		}
		else if (caseB) {
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (byte) backgroundValues[b];
							}
						}
					}
					else {
						// SG if we falls outside the roi we use the background
						// value
						if (!(roiBounds.contains(
								sx,
								sy) && roiIter.getSample(
								sx,
								sy,
								0) > 0)) {
							/* Fill with a background color. */
							if (setBackground) {
								for (int b = 0; b < dstBands; b++) {
									data[b][pixelOffset + bandOffsets[b]] = (byte) backgroundValues[b];
								}
							}
						}
						else {
							// Else the related source pixel is set
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (byte) (iter.getSample(
										sx,
										sy,
										b) & 0xFF);
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// ONLY NODATA
		}
		else if (caseC || (hasROI && hasNoData && roiContainsTile)) {
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (byte) backgroundValues[b];
							}
						}
					}
					else {
						// The related source pixel is set if it isn't a nodata
						for (int b = 0; b < dstBands; b++) {
							data[b][pixelOffset + bandOffsets[b]] = byteLookupTable[b][iter.getSample(
									sx,
									sy,
									b)];
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// BOTH ROI AND NODATA
		}
		else {
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (byte) backgroundValues[b];
							}
						}
					}
					else {
						// SG if we falls outside the roi we use the background
						// value
						if (!(roiBounds.contains(
								sx,
								sy) && roiBounds.contains(
								sx,
								sy) && roiIter.getSample(
								sx,
								sy,
								0) > 0)) {
							/* Fill with a background color. */
							if (setBackground) {
								for (int b = 0; b < dstBands; b++) {
									data[b][pixelOffset + bandOffsets[b]] = (byte) backgroundValues[b];
								}
							}
						}
						else {
							// The related source pixel is set if it isn't a
							// nodata
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = byteLookupTable[b][iter.getSample(
										sx,
										sy,
										b)];
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
		}
		iter.done();
	}

	protected void computeRectUShort(
			final PlanarImage src,
			final RasterAccessor dst,
			final RandomIter roiIter,
			boolean roiContainsTile ) {
		// Random Iterator on the source image bounds
		final RandomIter iter = RandomIterFactory.create(
				src,
				src.getBounds(),
				TILE_CACHED,
				ARRAY_CALC);
		// Initial settings
		final int minX = src.getMinX();
		final int maxX = src.getMaxX();
		final int minY = src.getMinY();
		final int maxY = src.getMaxY();

		final int dstWidth = dst.getWidth();
		final int dstHeight = dst.getHeight();
		final int dstBands = dst.getNumBands();

		final int lineStride = dst.getScanlineStride();
		final int pixelStride = dst.getPixelStride();
		final int[] bandOffsets = dst.getBandOffsets();
		final short[][] data = dst.getShortDataArrays();

		final float[] warpData = new float[2 * dstWidth];

		int lineOffset = 0;

		// NO ROI AND NODATA
		if (caseA || (caseB && roiContainsTile)) {
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);
					// If the pixel is outside the input image bounds
					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (short) backgroundValues[b];
							}
						}
					}
					else {
						// Nearest interpolation
						for (int b = 0; b < dstBands; b++) {
							data[b][pixelOffset + bandOffsets[b]] = (short) (iter.getSample(
									sx,
									sy,
									b) & 0xFFFF);
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// ONLY ROI
		}
		else if (caseB) {
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (short) backgroundValues[b];
							}
						}
					}
					else {
						// SG if we falls outside the roi we use the background
						// value
						if (!(roiBounds.contains(
								sx,
								sy) && roiBounds.contains(
								sx,
								sy) && roiIter.getSample(
								sx,
								sy,
								0) > 0)) {
							/* Fill with a background color. */
							if (setBackground) {
								for (int b = 0; b < dstBands; b++) {
									data[b][pixelOffset + bandOffsets[b]] = (short) backgroundValues[b];
								}
							}
						}
						else {
							// Else the related source pixel is set
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (short) (iter.getSample(
										sx,
										sy,
										b) & 0xFFFF);
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// ONLY NODATA
		}
		else if (caseC || (hasROI && hasNoData && roiContainsTile)) {
			short inputValue = 0;
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (short) backgroundValues[b];
							}
						}
					}
					else {
						// The related source pixel is set if it isn't a nodata
						for (int b = 0; b < dstBands; b++) {
							// Input value selected
							inputValue = (short) (iter.getSample(
									sx,
									sy,
									b) & 0xFFFF);
							if (noDataRange.contains(inputValue)) {
								data[b][pixelOffset + bandOffsets[b]] = (short) backgroundValues[b];
							}
							else {
								data[b][pixelOffset + bandOffsets[b]] = inputValue;
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// BOTH ROI AND NODATA
		}
		else {
			short inputValue = 0;
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (short) backgroundValues[b];
							}
						}
					}
					else {
						// SG if we falls outside the roi we use the background
						// value
						if (!(roiBounds.contains(
								sx,
								sy) && roiIter.getSample(
								sx,
								sy,
								0) > 0)) {
							/* Fill with a background color. */
							if (setBackground) {
								for (int b = 0; b < dstBands; b++) {
									data[b][pixelOffset + bandOffsets[b]] = (short) backgroundValues[b];
								}
							}
						}
						else {
							// The related source pixel is set if it isn't a
							// nodata
							for (int b = 0; b < dstBands; b++) {
								// Input value selected
								inputValue = (short) (iter.getSample(
										sx,
										sy,
										b) & 0xFFFF);
								if (noDataRange.contains(inputValue)) {
									data[b][pixelOffset + bandOffsets[b]] = (short) backgroundValues[b];
								}
								else {
									data[b][pixelOffset + bandOffsets[b]] = inputValue;
								}
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
		}
		iter.done();
	}

	protected void computeRectShort(
			final PlanarImage src,
			final RasterAccessor dst,
			final RandomIter roiIter,
			boolean roiContainsTile ) {
		// Random Iterator on the source image bounds
		final RandomIter iter = RandomIterFactory.create(
				src,
				src.getBounds(),
				TILE_CACHED,
				ARRAY_CALC);
		// Initial settings
		final int minX = src.getMinX();
		final int maxX = src.getMaxX();
		final int minY = src.getMinY();
		final int maxY = src.getMaxY();

		final int dstWidth = dst.getWidth();
		final int dstHeight = dst.getHeight();
		final int dstBands = dst.getNumBands();

		final int lineStride = dst.getScanlineStride();
		final int pixelStride = dst.getPixelStride();
		final int[] bandOffsets = dst.getBandOffsets();
		final short[][] data = dst.getShortDataArrays();

		final float[] warpData = new float[2 * dstWidth];

		int lineOffset = 0;

		// NO ROI AND NODATA
		if (caseA || (caseB && roiContainsTile)) {
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);
					// If the pixel is outside the input image bounds
					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (short) backgroundValues[b];
							}
						}
					}
					else {
						// Nearest interpolation
						for (int b = 0; b < dstBands; b++) {
							data[b][pixelOffset + bandOffsets[b]] = (short) iter.getSample(
									sx,
									sy,
									b);
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// ONLY ROI
		}
		else if (caseB) {
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (short) backgroundValues[b];
							}
						}
					}
					else {
						// SG if we falls outside the roi we use the background
						// value
						if (!(roiBounds.contains(
								sx,
								sy) && roiIter.getSample(
								sx,
								sy,
								0) > 0)) {
							/* Fill with a background color. */
							if (setBackground) {
								for (int b = 0; b < dstBands; b++) {
									data[b][pixelOffset + bandOffsets[b]] = (short) backgroundValues[b];
								}
							}
						}
						else {
							// Else the related source pixel is set
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (short) iter.getSample(
										sx,
										sy,
										b);
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// ONLY NODATA
		}
		else if (caseC || (hasROI && hasNoData && roiContainsTile)) {
			short inputValue = 0;
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (short) backgroundValues[b];
							}
						}
					}
					else {
						// The related source pixel is set if it isn't a nodata
						for (int b = 0; b < dstBands; b++) {
							// Input value selected
							inputValue = (short) iter.getSample(
									sx,
									sy,
									b);
							if (noDataRange.contains(inputValue)) {
								data[b][pixelOffset + bandOffsets[b]] = (short) backgroundValues[b];
							}
							else {
								data[b][pixelOffset + bandOffsets[b]] = inputValue;
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// BOTH ROI AND NODATA
		}
		else {
			short inputValue = 0;
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (short) backgroundValues[b];
							}
						}
					}
					else {
						// SG if we falls outside the roi we use the background
						// value
						if (!(roiBounds.contains(
								sx,
								sy) && roiIter.getSample(
								sx,
								sy,
								0) > 0)) {
							/* Fill with a background color. */
							if (setBackground) {
								for (int b = 0; b < dstBands; b++) {
									data[b][pixelOffset + bandOffsets[b]] = (short) backgroundValues[b];
								}
							}
						}
						else {
							// The related source pixel is set if it isn't a
							// nodata
							for (int b = 0; b < dstBands; b++) {
								// Input value selected
								inputValue = (short) iter.getSample(
										sx,
										sy,
										b);
								if (noDataRange.contains(inputValue)) {
									data[b][pixelOffset + bandOffsets[b]] = (short) backgroundValues[b];
								}
								else {
									data[b][pixelOffset + bandOffsets[b]] = inputValue;
								}
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
		}
		iter.done();
	}

	protected void computeRectInt(
			final PlanarImage src,
			final RasterAccessor dst,
			final RandomIter roiIter,
			boolean roiContainsTile ) {
		// Random Iterator on the source image bounds
		final RandomIter iter = RandomIterFactory.create(
				src,
				src.getBounds(),
				TILE_CACHED,
				ARRAY_CALC);
		// Initial settings
		final int minX = src.getMinX();
		final int maxX = src.getMaxX();
		final int minY = src.getMinY();
		final int maxY = src.getMaxY();

		final int dstWidth = dst.getWidth();
		final int dstHeight = dst.getHeight();
		final int dstBands = dst.getNumBands();

		final int lineStride = dst.getScanlineStride();
		final int pixelStride = dst.getPixelStride();
		final int[] bandOffsets = dst.getBandOffsets();
		final int[][] data = dst.getIntDataArrays();

		final float[] warpData = new float[2 * dstWidth];

		int lineOffset = 0;

		// NO ROI AND NODATA
		if (caseA || (caseB && roiContainsTile)) {
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);
					// If the pixel is outside the input image bounds
					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (int) backgroundValues[b];
							}
						}
					}
					else {
						// Nearest interpolation
						for (int b = 0; b < dstBands; b++) {
							data[b][pixelOffset + bandOffsets[b]] = iter.getSample(
									sx,
									sy,
									b);
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// ONLY ROI
		}
		else if (caseB) {
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (int) backgroundValues[b];
							}
						}
					}
					else {
						// SG if we falls outside the roi we use the background
						// value
						if (!(roiBounds.contains(
								sx,
								sy) && roiIter.getSample(
								sx,
								sy,
								0) > 0)) {
							/* Fill with a background color. */
							if (setBackground) {
								for (int b = 0; b < dstBands; b++) {
									data[b][pixelOffset + bandOffsets[b]] = (int) backgroundValues[b];
								}
							}
						}
						else {
							// Else the related source pixel is set
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = iter.getSample(
										sx,
										sy,
										b);
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// ONLY NODATA
		}
		else if (caseC || (hasROI && hasNoData && roiContainsTile)) {
			int inputValue = 0;
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (int) backgroundValues[b];
							}
						}
					}
					else {
						// The related source pixel is set if it isn't a nodata
						for (int b = 0; b < dstBands; b++) {
							// Input value selected
							inputValue = iter.getSample(
									sx,
									sy,
									b);
							if (noDataRange.contains(inputValue)) {
								data[b][pixelOffset + bandOffsets[b]] = (int) backgroundValues[b];
							}
							else {
								data[b][pixelOffset + bandOffsets[b]] = inputValue;
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// BOTH ROI AND NODATA
		}
		else {
			int inputValue = 0;
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (int) backgroundValues[b];
							}
						}
					}
					else {
						// SG if we falls outside the roi we use the background
						// value
						if (!(roiBounds.contains(
								sx,
								sy) && roiIter.getSample(
								sx,
								sy,
								0) > 0)) {
							/* Fill with a background color. */
							if (setBackground) {
								for (int b = 0; b < dstBands; b++) {
									data[b][pixelOffset + bandOffsets[b]] = (int) backgroundValues[b];
								}
							}
						}
						else {
							// The related source pixel is set if it isn't a
							// nodata
							for (int b = 0; b < dstBands; b++) {
								// Input value selected
								inputValue = iter.getSample(
										sx,
										sy,
										b);
								if (noDataRange.contains(inputValue)) {
									data[b][pixelOffset + bandOffsets[b]] = (int) backgroundValues[b];
								}
								else {
									data[b][pixelOffset + bandOffsets[b]] = inputValue;
								}
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
		}
		iter.done();
	}

	protected void computeRectFloat(
			final PlanarImage src,
			final RasterAccessor dst,
			final RandomIter roiIter,
			boolean roiContainsTile ) {
		// Random Iterator on the source image bounds
		final RandomIter iter = RandomIterFactory.create(
				src,
				src.getBounds(),
				TILE_CACHED,
				ARRAY_CALC);
		// Initial settings
		final int minX = src.getMinX();
		final int maxX = src.getMaxX();
		final int minY = src.getMinY();
		final int maxY = src.getMaxY();

		final int dstWidth = dst.getWidth();
		final int dstHeight = dst.getHeight();
		final int dstBands = dst.getNumBands();

		final int lineStride = dst.getScanlineStride();
		final int pixelStride = dst.getPixelStride();
		final int[] bandOffsets = dst.getBandOffsets();
		final float[][] data = dst.getFloatDataArrays();

		final float[] warpData = new float[2 * dstWidth];

		int lineOffset = 0;

		// NO ROI AND NODATA
		if (caseA || (caseB && roiContainsTile)) {
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);
					// If the pixel is outside the input image bounds
					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (float) backgroundValues[b];
							}
						}
					}
					else {
						// Nearest interpolation
						for (int b = 0; b < dstBands; b++) {
							data[b][pixelOffset + bandOffsets[b]] = iter.getSampleFloat(
									sx,
									sy,
									b);
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// ONLY ROI
		}
		else if (caseB) {
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (float) backgroundValues[b];
							}
						}
					}
					else {
						// SG if we falls outside the roi we use the background
						// value
						if (!(roiBounds.contains(
								sx,
								sy) && roiIter.getSample(
								sx,
								sy,
								0) > 0)) {
							/* Fill with a background color. */
							if (setBackground) {
								for (int b = 0; b < dstBands; b++) {
									data[b][pixelOffset + bandOffsets[b]] = (float) backgroundValues[b];
								}
							}
						}
						else {
							// Else the related source pixel is set
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = iter.getSampleFloat(
										sx,
										sy,
										b);
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// ONLY NODATA
		}
		else if (caseC || (hasROI && hasNoData && roiContainsTile)) {
			float inputValue = 0;
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (float) backgroundValues[b];
							}
						}
					}
					else {
						// The related source pixel is set if it isn't a nodata
						for (int b = 0; b < dstBands; b++) {
							// Input value selected
							inputValue = iter.getSampleFloat(
									sx,
									sy,
									b);
							if (noDataRange.contains(inputValue)) {
								data[b][pixelOffset + bandOffsets[b]] = (float) backgroundValues[b];
							}
							else {
								data[b][pixelOffset + bandOffsets[b]] = inputValue;
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// BOTH ROI AND NODATA
		}
		else {
			float inputValue = 0;
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = (float) backgroundValues[b];
							}
						}
					}
					else {
						// SG if we falls outside the roi we use the background
						// value
						if (!(roiBounds.contains(
								sx,
								sy) && roiIter.getSample(
								sx,
								sy,
								0) > 0)) {
							/* Fill with a background color. */
							if (setBackground) {
								for (int b = 0; b < dstBands; b++) {
									data[b][pixelOffset + bandOffsets[b]] = (float) backgroundValues[b];
								}
							}
						}
						else {
							// The related source pixel is set if it isn't a
							// nodata
							for (int b = 0; b < dstBands; b++) {
								// Input value selected
								inputValue = iter.getSampleFloat(
										sx,
										sy,
										b);
								if (noDataRange.contains(inputValue)) {
									data[b][pixelOffset + bandOffsets[b]] = (float) backgroundValues[b];
								}
								else {
									data[b][pixelOffset + bandOffsets[b]] = inputValue;
								}
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
		}
		iter.done();
	}

	protected void computeRectDouble(
			final PlanarImage src,
			final RasterAccessor dst,
			final RandomIter roiIter,
			boolean roiContainsTile ) {
		// Random Iterator on the source image bounds
		final RandomIter iter = RandomIterFactory.create(
				src,
				src.getBounds(),
				TILE_CACHED,
				ARRAY_CALC);
		// Initial settings
		final int minX = src.getMinX();
		final int maxX = src.getMaxX();
		final int minY = src.getMinY();
		final int maxY = src.getMaxY();

		final int dstWidth = dst.getWidth();
		final int dstHeight = dst.getHeight();
		final int dstBands = dst.getNumBands();

		final int lineStride = dst.getScanlineStride();
		final int pixelStride = dst.getPixelStride();
		final int[] bandOffsets = dst.getBandOffsets();
		final double[][] data = dst.getDoubleDataArrays();

		final float[] warpData = new float[2 * dstWidth];

		int lineOffset = 0;

		// NO ROI AND NODATA
		if (caseA || (caseB && roiContainsTile)) {
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);
					// If the pixel is outside the input image bounds
					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = backgroundValues[b];
							}
						}
					}
					else {
						// Nearest interpolation
						for (int b = 0; b < dstBands; b++) {
							data[b][pixelOffset + bandOffsets[b]] = iter.getSampleDouble(
									sx,
									sy,
									b);
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// ONLY ROI
		}
		else if (caseB) {
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = backgroundValues[b];
							}
						}
					}
					else {
						// SG if we falls outside the roi we use the background
						// value
						if (!(roiBounds.contains(
								sx,
								sy) && roiIter.getSample(
								sx,
								sy,
								0) > 0)) {
							/* Fill with a background color. */
							if (setBackground) {
								for (int b = 0; b < dstBands; b++) {
									data[b][pixelOffset + bandOffsets[b]] = backgroundValues[b];
								}
							}
						}
						else {
							// Else the related source pixel is set
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = iter.getSampleDouble(
										sx,
										sy,
										b);
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// ONLY NODATA
		}
		else if (caseC || (hasROI && hasNoData && roiContainsTile)) {
			double inputValue = 0;
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = backgroundValues[b];
							}
						}
					}
					else {
						// The related source pixel is set if it isn't a nodata
						for (int b = 0; b < dstBands; b++) {
							// Input value selected
							inputValue = iter.getSampleDouble(
									sx,
									sy,
									b);
							if (noDataRange.contains(inputValue)) {
								data[b][pixelOffset + bandOffsets[b]] = backgroundValues[b];
							}
							else {
								data[b][pixelOffset + bandOffsets[b]] = inputValue;
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
			// BOTH ROI AND NODATA
		}
		else {
			double inputValue = 0;
			for (int h = 0; h < dstHeight; h++) {
				int pixelOffset = lineOffset;
				lineOffset += lineStride;
				// Calculation of the warp for the selected row
				warp.warpRect(
						dst.getX(),
						dst.getY() + h,
						dstWidth,
						1,
						warpData);
				int count = 0;
				for (int w = 0; w < dstWidth; w++) {
					/*
					 * The warp object subtract 0.5 from backward mapped source
					 * coordinate. Need to do a round to get the nearest
					 * neighbor. This is different from the standard nearest
					 * implementation.
					 */
					final int sx = round(warpData[count++]);
					final int sy = round(warpData[count++]);

					if (sx < minX || sx >= maxX || sy < minY || sy >= maxY) {
						/* Fill with a background color. */
						if (setBackground) {
							for (int b = 0; b < dstBands; b++) {
								data[b][pixelOffset + bandOffsets[b]] = backgroundValues[b];
							}
						}
					}
					else {
						// SG if we falls outside the roi we use the background
						// value
						if (!(roiBounds.contains(
								sx,
								sy) && roiIter.getSample(
								sx,
								sy,
								0) > 0)) {
							/* Fill with a background color. */
							if (setBackground) {
								for (int b = 0; b < dstBands; b++) {
									data[b][pixelOffset + bandOffsets[b]] = backgroundValues[b];
								}
							}
						}
						else {
							// The related source pixel is set if it isn't a
							// nodata
							for (int b = 0; b < dstBands; b++) {
								// Input value selected
								inputValue = iter.getSampleDouble(
										sx,
										sy,
										b);
								if (noDataRange.contains(inputValue)) {
									data[b][pixelOffset + bandOffsets[b]] = backgroundValues[b];
								}
								else {
									data[b][pixelOffset + bandOffsets[b]] = inputValue;
								}
							}
						}
					}
					pixelOffset += pixelStride;
				}
			}
		}
		iter.done();
	}
}
