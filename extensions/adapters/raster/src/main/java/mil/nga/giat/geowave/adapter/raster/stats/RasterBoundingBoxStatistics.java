package mil.nga.giat.geowave.adapter.raster.stats;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.adapter.raster.FitToIndexGridCoverage;
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.geotools.geometry.GeneralEnvelope;
import org.opengis.coverage.grid.GridCoverage;

import com.vividsolutions.jts.geom.Envelope;

public class RasterBoundingBoxStatistics extends
		BoundingBoxDataStatistics<GridCoverage>
{
	protected RasterBoundingBoxStatistics() {
		super();
	}

	public RasterBoundingBoxStatistics(
			final ByteArrayId dataAdapterId ) {
		super(
				dataAdapterId);
	}

	@Override
	protected Envelope getEnvelope(
			final GridCoverage entry ) {
		final org.opengis.geometry.Envelope indexedEnvelope = entry.getEnvelope();
		final org.opengis.geometry.Envelope originalEnvelope;
		if (entry instanceof FitToIndexGridCoverage) {
			originalEnvelope = ((FitToIndexGridCoverage) entry).getOriginalEnvelope();
		}
		else {
			originalEnvelope = null;
		}
		// we don't want to accumulate the envelope outside of the original if
		// it is fit to the index, so compute the intersection with the original
		// envelope
		final org.opengis.geometry.Envelope resultingEnvelope = getIntersection(
				originalEnvelope,
				indexedEnvelope);
		if (resultingEnvelope != null) {
			return new Envelope(
					resultingEnvelope.getMinimum(0),
					resultingEnvelope.getMaximum(0),
					resultingEnvelope.getMinimum(1),
					resultingEnvelope.getMaximum(1));
		}
		return null;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buffer = ByteBuffer.allocate(32);
		buffer.putDouble(minX);
		buffer.putDouble(minY);
		buffer.putDouble(maxX);
		buffer.putDouble(maxY);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		minX = buffer.getDouble();
		minY = buffer.getDouble();
		maxX = buffer.getDouble();
		maxY = buffer.getDouble();
	}

	private static org.opengis.geometry.Envelope getIntersection(
			final org.opengis.geometry.Envelope originalEnvelope,
			final org.opengis.geometry.Envelope indexedEnvelope ) {
		if (originalEnvelope == null) {
			return indexedEnvelope;
		}
		if (indexedEnvelope == null) {
			return originalEnvelope;
		}
		final int dimensions = originalEnvelope.getDimension();
		final double[] minDP = new double[dimensions];
		final double[] maxDP = new double[dimensions];
		for (int d = 0; d < dimensions; d++) {
			// to perform the intersection of the original envelope and the
			// indexed envelope, use the max of the mins per dimension and the
			// min of the maxes
			minDP[d] = Math.max(
					originalEnvelope.getMinimum(d),
					indexedEnvelope.getMinimum(d));
			maxDP[d] = Math.min(
					originalEnvelope.getMaximum(d),
					indexedEnvelope.getMaximum(d));
		}
		return new GeneralEnvelope(
				minDP,
				maxDP);
	}
}
