package mil.nga.giat.geowave.adapter.vector.wms.accumulo;

import java.awt.image.BufferedImage;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.PersistenceUtils;

/**
 * In addition to a main persisted image, this class also wraps rendered styles.
 * The main image contains any labels, and the styles each persist a per-style
 * image so that all of the images can be correctly layered from each Accumulo
 * Iterator for the composite image.
 * 
 */
public class RenderedMaster extends
		PersistableRenderedImage
{
	private List<RenderedStyle> renderedStyles = new ArrayList<RenderedStyle>();

	protected RenderedMaster() {}

	public RenderedMaster(
			final List<ServerFeatureStyle> styles,
			final BufferedImage image ) {
		super(
				image);

		for (final ServerFeatureStyle s : styles) {
			if (s.getRenderedStyle().getImage() != null) {
				renderedStyles.add(s.getRenderedStyle());
			}
		}
	}

	public List<RenderedStyle> getRenderedStyles() {
		return renderedStyles;
	}

	@Override
	public byte[] toBinary() {
		final byte[] selfBinary = super.toBinary();
		final List<byte[]> styleBinaries = new ArrayList<byte[]>(
				renderedStyles.size());
		int styleBinaryLength = 0;
		for (final RenderedStyle style : renderedStyles) {
			final byte[] binary = PersistenceUtils.toBinary(style);
			styleBinaries.add(binary);
			styleBinaryLength += (binary.length + 4);
		}

		final ByteBuffer buf = ByteBuffer.allocate(8 + styleBinaryLength + selfBinary.length);
		buf.putInt(selfBinary.length);
		buf.put(selfBinary);
		buf.putInt(styleBinaries.size());
		for (final byte[] styleBinary : styleBinaries) {
			buf.putInt(styleBinary.length);
			buf.put(styleBinary);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int masterLength = buf.getInt();
		final byte[] masterBytes = new byte[masterLength];
		buf.get(masterBytes);

		super.fromBinary(masterBytes);
		final int numStyles = buf.getInt();
		renderedStyles = new ArrayList<RenderedStyle>(
				numStyles);
		for (int i = 0; i < numStyles; i++) {
			final int styleBinaryLength = buf.getInt();
			final byte[] styleBinary = new byte[styleBinaryLength];
			buf.get(styleBinary);
			renderedStyles.add(PersistenceUtils.fromBinary(
					styleBinary,
					RenderedStyle.class));
		}
	}

}
