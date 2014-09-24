package mil.nga.giat.geowave.raster.stats;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.adapter.statistics.BoundingBoxDataStatistics;

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
		final org.opengis.geometry.Envelope env = entry.getEnvelope();
		if (env != null) {
			return new Envelope(
					env.getMinimum(0),
					env.getMaximum(0),
					env.getMinimum(1),
					env.getMaximum(1));
		}
		return null;
	}
}
