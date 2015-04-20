package mil.nga.giat.geowave.adapter.vector.query.row;

import java.awt.geom.NoninvertibleTransformException;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.NumericIndexStrategy;

import org.geotools.geometry.jts.ReferencedEnvelope;

/**
 * This extends the concept of the BasicRowIdStore which tracks painted pixels
 * and their mapping to row IDs, but it only marks a pixel (or cell of pixels)
 * as 'painted' once it has exceeded a threshold number of times rendered.
 * 
 */
public class CountThresholdRowIdStore extends
		BasicRowIdStore
{
	private final int maxCount;
	private final Map<Integer, Integer> pixelIndexToCountMap = new HashMap<Integer, Integer>();

	public CountThresholdRowIdStore(
			final int width,
			final int height,
			final NumericIndexStrategy indexStrategy,
			final ReferencedEnvelope env,
			final int maxCount,
			final Integer pixelSize )
			throws NoninvertibleTransformException {
		super(
				width,
				height,
				indexStrategy,
				env,
				pixelSize);
		this.maxCount = maxCount;
	}

	@Override
	public void notifyPixelPainted(
			final int x,
			final int y,
			final boolean shouldDecimate ) {
		final int pixelIndex = getPixelIndex(
				x,
				y);
		if (shouldDecimate) {
			pixelIndexToCountMap.remove(pixelIndex);
			decimate(pixelIndex);
		}
		else {
			// increment counts
			Integer count = pixelIndexToCountMap.get(pixelIndex);
			if (count == null) {
				count = 0;
			}
			count++;
			if (count < maxCount) {
				pixelIndexToCountMap.put(
						pixelIndex,
						count);
			}
			else {
				pixelIndexToCountMap.remove(pixelIndex);
				decimate(pixelIndex);
			}
		}
	}

}
