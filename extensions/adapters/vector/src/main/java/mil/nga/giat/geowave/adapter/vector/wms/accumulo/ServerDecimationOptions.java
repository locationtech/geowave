package mil.nga.giat.geowave.adapter.vector.wms.accumulo;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.Persistable;

/**
 * This class is responsible for persisting any options available to decimation
 * when performing distributed rendering. One option includes a flag indicating
 * whether to decimate based on secondary styles (if any style renders to a
 * pixel, use the decimation) or just use the last style (the one that is
 * rendered last, layered on top technically should drive decimation but
 * performance could suffer for technical accuracy). Another option includes max
 * alpha which can be set to only decimate when the opacity of a pixel exceeds a
 * threshold. Another option, maxCount, can be set to only decimate on a pixel
 * if its been rendered to more than a set number of times. Another option is
 * pixelSize which can improve performance by decimated on multi-pixel cells
 * rather than single pizels (eg. if pixelSize is 5, it would decimate on 5x5
 * pixel cells).
 * 
 */
public class ServerDecimationOptions implements
		Persistable
{
	private boolean useSecondaryStyles;
	private Double maxAlpha = null;
	private Integer maxCount = null;
	private Integer pixelSize = null;

	protected ServerDecimationOptions() {}

	public ServerDecimationOptions(
			final boolean useSecondaryStyles,
			final Double maxAlpha,
			final Integer maxCount,
			final Integer pixelSize ) {
		this.useSecondaryStyles = useSecondaryStyles;
		this.maxAlpha = maxAlpha;
		this.maxCount = maxCount;
		this.pixelSize = pixelSize;
	}

	public boolean isUseSecondaryStyles() {
		return useSecondaryStyles;
	}

	public Double getMaxAlpha() {
		return maxAlpha;
	}

	public Integer getMaxCount() {
		return maxCount;
	}

	public Integer getPixelSize() {
		return pixelSize;
	}

	@Override
	public byte[] toBinary() {
		int totalBytes = 4;
		if (maxAlpha != null) {
			totalBytes += 8;
		}
		if (maxCount != null) {
			totalBytes += 4;
		}
		if (pixelSize != null) {
			totalBytes += 4;
		}
		final ByteBuffer buf = ByteBuffer.allocate(totalBytes);
		buf.put((byte) (useSecondaryStyles ? 1 : 0));
		buf.put((byte) ((maxAlpha != null) ? 1 : 0));
		if (maxAlpha != null) {
			buf.putDouble(maxAlpha);
		}
		buf.put((byte) ((maxCount != null) ? 1 : 0));
		if (maxCount != null) {
			buf.putInt(maxCount);
		}
		buf.put((byte) ((pixelSize != null) ? 1 : 0));
		if (pixelSize != null) {
			buf.putInt(pixelSize);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		useSecondaryStyles = buf.get() > 0;
		final boolean maxAlphaMask = buf.get() > 0;
		if (maxAlphaMask) {
			maxAlpha = buf.getDouble();
		}
		else {
			maxAlpha = null;
		}
		final boolean maxCountMask = buf.get() > 0;
		if (maxCountMask) {
			maxCount = buf.getInt();
		}
		else {
			maxCount = null;
		}
		if (!maxAlphaMask && !maxCountMask) {
			// if we don't have either, we should fallback to a single count
			maxCount = 1;
		}
		final boolean pixelSizeMask = buf.get() > 0;
		if (pixelSizeMask) {
			pixelSize = buf.getInt();
		}
		else {
			pixelSize = null;
		}
	}
}
