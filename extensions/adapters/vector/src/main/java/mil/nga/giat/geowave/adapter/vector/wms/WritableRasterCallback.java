package mil.nga.giat.geowave.adapter.vector.wms;

import java.awt.image.BufferedImage;

/**
 * This interface can be given to an EventingWritableRaster to receive
 * notifications as pixels are written to an AWT WritableRaster.
 * 
 */
public interface WritableRasterCallback
{
	public void pixelChanged(
			int x,
			int y );

	public void pixelChanged(
			int x,
			int y,
			int w,
			int h );

	public void setImage(
			BufferedImage image );
}
