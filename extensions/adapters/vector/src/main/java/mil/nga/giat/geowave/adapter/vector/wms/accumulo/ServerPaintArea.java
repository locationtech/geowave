package mil.nga.giat.geowave.adapter.vector.wms.accumulo;

import java.awt.Rectangle;
import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.Persistable;

/**
 * This class persists information about the image (width, height).
 * 
 */
public class ServerPaintArea implements
		Persistable
{
	private int width;
	private int height;

	protected ServerPaintArea() {}

	public ServerPaintArea(
			final int width,
			final int height ) {
		this.width = width;
		this.height = height;
	}

	public int getMinX() {
		return 0;
	}

	public int getMinY() {
		return 0;
	}

	public int getMaxX() {
		return width;
	}

	public int getMaxY() {
		return height;
	}

	public Rectangle getArea() {
		return new Rectangle(
				0,
				0,
				width,
				height);
	}

	public int getWidth() {
		return width;
	}

	public int getHeight() {
		return height;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = ByteBuffer.allocate(8);
		buf.putInt(width);
		buf.putInt(height);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		width = buf.getInt();
		height = buf.getInt();
	}

}
