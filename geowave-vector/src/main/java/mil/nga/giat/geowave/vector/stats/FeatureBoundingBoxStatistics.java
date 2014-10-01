package mil.nga.giat.geowave.vector.stats;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.adapter.statistics.BoundingBoxDataStatistics;

import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

public class FeatureBoundingBoxStatistics extends
		BoundingBoxDataStatistics<SimpleFeature>
{

	protected FeatureBoundingBoxStatistics() {
		super();
	}

	public FeatureBoundingBoxStatistics(
			final ByteArrayId dataAdapterId ) {
		super(
				dataAdapterId);
	}

	@Override
	protected Envelope getEnvelope(
			final SimpleFeature entry ) {
		// incorporate the bounding box of the entry's envelope
		final Geometry geometry = (Geometry) entry.getDefaultGeometry();
		if ((geometry != null) && !geometry.isEmpty()) {
			return geometry.getEnvelopeInternal();
		}
		return null;
	}

}
