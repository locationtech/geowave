package mil.nga.giat.geowave.adapter.vector.wms;

import java.awt.Point;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;

/**
 * This class wraps an AWT WritableRaster with a callback to be able to notify
 * when pixels are written for the raster. All methods are delegated to the
 * underlying writable raster.
 * 
 */
public class EventingWritableRaster extends
		WritableRaster
{
	private final WritableRasterCallback callback;

	public EventingWritableRaster(
			final WritableRaster delegate,
			final WritableRasterCallback callback ) {
		super(
				delegate.getSampleModel(),
				delegate.getDataBuffer(),
				delegate.getBounds(),
				new Point(
						delegate.getSampleModelTranslateX(),
						delegate.getSampleModelTranslateY()),
				(WritableRaster) delegate.getParent());
		this.callback = callback;
	}

	@Override
	public void setDataElements(
			final int x,
			final int y,
			final int w,
			final int h,
			final Object inData ) {
		super.setDataElements(
				x,
				y,
				w,
				h,
				inData);
		callback.pixelChanged(
				x,
				y,
				w,
				h);
	}

	@Override
	public void setDataElements(
			final int x,
			final int y,
			final Object inData ) {
		super.setDataElements(
				x,
				y,
				inData);
		callback.pixelChanged(
				x,
				y);
	}

	@Override
	public void setDataElements(
			final int x,
			final int y,
			final Raster inRaster ) {
		super.setDataElements(
				x,
				y,
				inRaster);
		callback.pixelChanged(
				x,
				y);
	}

	@Override
	public void setPixel(
			final int x,
			final int y,
			final double[] dArray ) {
		super.setPixel(
				x,
				y,
				dArray);
		callback.pixelChanged(
				x,
				y);
	}

	@Override
	public void setPixel(
			final int x,
			final int y,
			final float[] fArray ) {
		super.setPixel(
				x,
				y,
				fArray);
		callback.pixelChanged(
				x,
				y);
	}

	@Override
	public void setPixel(
			final int x,
			final int y,
			final int[] iArray ) {
		super.setPixel(
				x,
				y,
				iArray);
		callback.pixelChanged(
				x,
				y);
	}

	@Override
	public void setPixels(
			final int x,
			final int y,
			final int w,
			final int h,
			final double[] dArray ) {
		super.setPixels(
				x,
				y,
				w,
				h,
				dArray);
		callback.pixelChanged(
				x,
				y,
				w,
				h);
	}

	@Override
	public void setPixels(
			final int x,
			final int y,
			final int w,
			final int h,
			final float[] fArray ) {
		super.setPixels(
				x,
				y,
				w,
				h,
				fArray);
		callback.pixelChanged(
				x,
				y,
				w,
				h);
	}

	@Override
	public void setPixels(
			final int x,
			final int y,
			final int w,
			final int h,
			final int[] iArray ) {
		super.setPixels(
				x,
				y,
				w,
				h,
				iArray);
		callback.pixelChanged(
				x,
				y,
				w,
				h);
	}

	@Override
	public void setRect(
			final int dx,
			final int dy,
			final Raster srcRaster ) {
		super.setRect(
				dx,
				dy,
				srcRaster);
		final int x = Math.max(
				0,
				dx);
		final int y = Math.max(
				0,
				dy);
		final int w = Math.min(
				getWidth() - x,
				srcRaster.getWidth() + dx);
		final int h = Math.min(
				getHeight() - y,
				srcRaster.getHeight() + dy);
		callback.pixelChanged(
				x,
				y,
				w,
				h);
	}

	@Override
	public void setRect(
			final Raster srcRaster ) {
		super.setRect(srcRaster);
		final int w = Math.min(
				getWidth(),
				srcRaster.getWidth());
		final int h = Math.min(
				getHeight(),
				srcRaster.getHeight());
		callback.pixelChanged(
				0,
				0,
				w,
				h);
	}

	@Override
	public void setSample(
			final int x,
			final int y,
			final int b,
			final double s ) {
		super.setSample(
				x,
				y,
				b,
				s);
		callback.pixelChanged(
				x,
				y);
	}

	@Override
	public void setSample(
			final int x,
			final int y,
			final int b,
			final float s ) {
		super.setSample(
				x,
				y,
				b,
				s);
		callback.pixelChanged(
				x,
				y);
	}

	@Override
	public void setSample(
			final int x,
			final int y,
			final int b,
			final int s ) {
		super.setSample(
				x,
				y,
				b,
				s);
		callback.pixelChanged(
				x,
				y);
	}

	@Override
	public void setSamples(
			final int x,
			final int y,
			final int w,
			final int h,
			final int b,
			final double[] dArray ) {
		super.setSamples(
				x,
				y,
				w,
				h,
				b,
				dArray);
		callback.pixelChanged(
				x,
				y,
				w,
				h);
	}

	@Override
	public void setSamples(
			final int x,
			final int y,
			final int w,
			final int h,
			final int b,
			final float[] fArray ) {
		super.setSamples(
				x,
				y,
				w,
				h,
				b,
				fArray);

		callback.pixelChanged(
				x,
				y,
				w,
				h);
	}

	@Override
	public void setSamples(
			final int x,
			final int y,
			final int w,
			final int h,
			final int b,
			final int[] iArray ) {
		super.setSamples(
				x,
				y,
				w,
				h,
				b,
				iArray);

		callback.pixelChanged(
				x,
				y,
				w,
				h);
	}
}
