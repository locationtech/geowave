package mil.nga.giat.geowave.adapter.vector.query.row;

import java.awt.Color;
import java.awt.image.BufferedImage;

import mil.nga.giat.geowave.adapter.vector.wms.WritableRasterCallback;

/**
 * This class ties a row ID store to notification events for all pixels that are
 * rendered on an image. It can also handle a maxAlpha (which can be null) which
 * will only pass through pixel paint notifications once an alpha threshold has
 * been exceeded for the pixels. Alpha in this case is a decimal value between 0
 * and 1. For example, if maxAlpha is given as 0.8, pixel paint notifications
 * will only be delegated to the row ID store when the pixel has an opacity more
 * than 80%. If maxAlpha is null, its the equivalent of 0, or all pixel paint
 * notifications are delegated to the row ID store.
 * 
 */
public class ImageDecimator implements
		WritableRasterCallback
{
	private final BasicRowIdStore rowIdStore;
	private final Double maxAlpha;
	private BufferedImage image;

	public ImageDecimator(
			final BasicRowIdStore rowIdStore,
			final Double maxAlpha ) {
		this.rowIdStore = rowIdStore;
		this.maxAlpha = maxAlpha;
	}

	public ImageDecimator(
			final BasicRowIdStore rowIdStore,
			final Double maxAlpha,
			final BufferedImage image ) {
		this.rowIdStore = rowIdStore;
		this.maxAlpha = maxAlpha;
		this.image = image;
	}

	@Override
	public void pixelChanged(
			final int x,
			final int y ) {
		rowIdStore.notifyPixelPainted(
				x,
				y,
				shouldDecimate(
						x,
						y));
	}

	private boolean shouldDecimate(
			final int x,
			final int y ) {
		boolean opacityResult = false;
		if (maxAlpha != null) {
			if (image == null) {
				// hopefully this is not the case
				return true;
			}
			opacityResult = (new Color(
					image.getRGB(
							x,
							y),
					true).getAlpha() / 255.0) > maxAlpha;
		}
		return opacityResult;
	}

	@Override
	public void pixelChanged(
			final int x,
			final int y,
			final int w,
			final int h ) {
		// assume this callback doesn't go outside of the bounds of the image
		for (int i = x; i < (x + w); i++) {
			for (int j = y; j < (y + h); j++) {
				rowIdStore.notifyPixelPainted(
						i,
						j,
						shouldDecimate(
								i,
								j));
			}
		}
	}

	@Override
	public void setImage(
			final BufferedImage image ) {
		this.image = image;
	}
}
