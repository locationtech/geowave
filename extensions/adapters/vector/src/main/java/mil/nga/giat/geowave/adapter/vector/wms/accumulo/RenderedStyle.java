package mil.nga.giat.geowave.adapter.vector.wms.accumulo;

import java.awt.image.BufferedImage;
import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.StringUtils;

/**
 * This class represents a persisted BufferedImage with a given style ID so that
 * it can be matched to the appropriate style ID for rendering in the correct
 * order in the composite image.
 * 
 */
public class RenderedStyle extends
		PersistableRenderedImage
{
	private String styleId;

	protected RenderedStyle() {}

	public RenderedStyle(
			final String styleId,
			final BufferedImage image ) {
		super(
				image);
		this.styleId = styleId;
	}

	public String getStyleId() {
		return styleId;
	}

	@Override
	public byte[] toBinary() {
		final byte[] imageBinary = super.toBinary();
		final byte[] styleIdBinary = StringUtils.stringToBinary(styleId);
		final ByteBuffer buf = ByteBuffer.allocate(imageBinary.length + styleIdBinary.length + 4);
		buf.putInt(imageBinary.length);
		buf.put(imageBinary);
		buf.put(styleIdBinary);

		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int imageBinaryLength = buf.getInt();
		final byte[] imageBinary = new byte[imageBinaryLength];
		buf.get(imageBinary);
		super.fromBinary(imageBinary);
		final byte[] styleIdBinary = new byte[bytes.length - 4 - imageBinaryLength];
		buf.get(styleIdBinary);
		styleId = StringUtils.stringFromBinary(styleIdBinary);
	}

}
