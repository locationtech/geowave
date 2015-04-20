package mil.nga.giat.geowave.adapter.vector.wms.accumulo;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import mil.nga.giat.geowave.core.index.Persistable;

import org.apache.log4j.Logger;

/**
 * This class wraps a rendered image as a GeoWave Persistable object. It
 * serializes and deserializes the BufferedImage as a png using ImageIO.
 * 
 */
abstract public class PersistableRenderedImage implements
		Persistable
{
	private final static Logger LOGGER = Logger.getLogger(PersistableRenderedImage.class);
	public BufferedImage image;

	protected PersistableRenderedImage() {}

	public PersistableRenderedImage(
			final BufferedImage image ) {
		this.image = image;
	}

	public BufferedImage getImage() {
		return image;
	}

	@Override
	public byte[] toBinary() {
		if (image == null) {
			return new byte[0];
		}
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			ImageIO.write(
					image,
					"png",
					baos);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to serialize image",
					e);
		}
		return baos.toByteArray();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if (bytes.length == 0) {
			return;
		}
		final ByteArrayInputStream bais = new ByteArrayInputStream(
				bytes);
		try {
			image = ImageIO.read(bais);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to deserialize image",
					e);
		}
	}

}
