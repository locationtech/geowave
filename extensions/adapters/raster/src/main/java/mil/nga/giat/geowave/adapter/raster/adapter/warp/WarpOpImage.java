package mil.nga.giat.geowave.adapter.raster.adapter.warp;

import java.awt.Rectangle;
import java.awt.image.DataBuffer;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.util.Map;

import javax.media.jai.BorderExtender;
import javax.media.jai.ImageLayout;
import javax.media.jai.Interpolation;
import javax.media.jai.PlanarImage;
import javax.media.jai.ROI;
import javax.media.jai.ROIShape;
import javax.media.jai.RasterAccessor;
import javax.media.jai.RasterFormatTag;
import javax.media.jai.Warp;
import javax.media.jai.iterator.RandomIter;

import com.sun.media.jai.util.ImageUtil;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import it.geosolutions.jaiext.iterators.RandomIterFactory;
import it.geosolutions.jaiext.range.Range;

/**
 * 
 * This is code entirely intended to get around an issue on line 265 of
 * WarpOpImage in jai-ext. The following code does not work if the source is
 * significant lower resolution than the destination and seems unnecessary in
 * general:
 * 
 * roiTile = roi.intersect(new ROIShape(srcRectExpanded));
 *
 */
@SuppressFBWarnings
abstract public class WarpOpImage extends
		it.geosolutions.jaiext.warp.WarpOpImage
{

	public WarpOpImage(
			RenderedImage source,
			ImageLayout layout,
			Map<?, ?> configuration,
			boolean cobbleSources,
			BorderExtender extender,
			Interpolation interp,
			Warp warp,
			double[] backgroundValues,
			ROI roi,
			Range noData ) {
		super(
				source,
				layout,
				configuration,
				cobbleSources,
				extender,
				interp,
				warp,
				backgroundValues,
				roi,
				noData);
	}

	/**
	 * Warps a rectangle. If ROI is present, the intersection between ROI and
	 * tile bounds is calculated; The result ROI will be used for calculations
	 * inside the computeRect() method.
	 */
	protected void computeRect(
			final PlanarImage[] sources,
			final WritableRaster dest,
			final Rectangle destRect ) {
		// Retrieve format tags.
		final RasterFormatTag[] formatTags = getFormatTags();

		final RasterAccessor dst = new RasterAccessor(
				dest,
				destRect,
				formatTags[1],
				getColorModel());

		RandomIter roiIter = null;

		boolean roiContainsTile = false;
		boolean roiDisjointTile = false;

		// If a ROI is present, then only the part contained inside the current
		// tile bounds is taken.
		if (hasROI) {
			Rectangle srcRectExpanded = mapDestRect(
					destRect,
					0);
			// The tile dimension is extended for avoiding border errors
			srcRectExpanded.setRect(
					srcRectExpanded.getMinX() - leftPad,
					srcRectExpanded.getMinY() - topPad,
					srcRectExpanded.getWidth() + rightPad + leftPad,
					srcRectExpanded.getHeight() + bottomPad + topPad);

			if (!roiBounds.intersects(srcRectExpanded)) {
				roiDisjointTile = true;
			}
			else {
				roiContainsTile = roi.contains(srcRectExpanded);
				if (!roiContainsTile) {
					if (!roi.intersects(srcRectExpanded)) {
						roiDisjointTile = true;
					}
					else {
						PlanarImage roiIMG = getImage();
						roiIter = RandomIterFactory.create(
								roiIMG,
								null,
								TILE_CACHED,
								ARRAY_CALC);
					}
				}
			}
		}

		if (!hasROI || !roiDisjointTile) {
			switch (dst.getDataType()) {
				case DataBuffer.TYPE_BYTE:
					computeRectByte(
							sources[0],
							dst,
							roiIter,
							roiContainsTile);
					break;
				case DataBuffer.TYPE_USHORT:
					computeRectUShort(
							sources[0],
							dst,
							roiIter,
							roiContainsTile);
					break;
				case DataBuffer.TYPE_SHORT:
					computeRectShort(
							sources[0],
							dst,
							roiIter,
							roiContainsTile);
					break;
				case DataBuffer.TYPE_INT:
					computeRectInt(
							sources[0],
							dst,
							roiIter,
							roiContainsTile);
					break;
				case DataBuffer.TYPE_FLOAT:
					computeRectFloat(
							sources[0],
							dst,
							roiIter,
							roiContainsTile);
					break;
				case DataBuffer.TYPE_DOUBLE:
					computeRectDouble(
							sources[0],
							dst,
							roiIter,
							roiContainsTile);
					break;
			}
			// After the calculations, the output data are copied into the
			// WritableRaster
			if (dst.isDataCopy()) {
				dst.clampDataArrays();
				dst.copyDataToRaster();
			}
		}
		else {
			// If the tile is outside the ROI, then the destination Raster is
			// set to backgroundValues
			if (setBackground) {
				ImageUtil.fillBackground(
						dest,
						destRect,
						backgroundValues);
			}
		}
	}

	/**
	 * This method provides a lazy initialization of the image associated to the
	 * ROI. The method uses the Double-checked locking in order to maintain
	 * thread-safety
	 * 
	 * @return
	 */
	private PlanarImage getImage() {
		PlanarImage img = roiImage;
		if (img == null) {
			synchronized (this) {
				img = roiImage;
				if (img == null) {
					roiImage = img = roi.getAsImage();
				}
			}
		}
		return img;
	}

}
