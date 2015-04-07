package mil.nga.giat.geowave.vector.stats;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.adapter.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.vector.util.FeatureDataUtils;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class FeatureBoundingBoxStatistics extends
		BoundingBoxDataStatistics<SimpleFeature> implements
		FeatureStatistic
{

	private static final String STATS_TYPE = "BBOX";

	private SimpleFeatureType persistedType;
	private SimpleFeatureType reprojectedType;
	private MathTransform transform;

	protected FeatureBoundingBoxStatistics() {
		super();
	}

	public FeatureBoundingBoxStatistics(
			final ByteArrayId dataAdapterId,
			final String fieldName ) {
		this(
				dataAdapterId,
				fieldName,
				null,
				null,
				null);
	}

	public FeatureBoundingBoxStatistics(
			final ByteArrayId dataAdapterId,
			final String fieldName,
			final SimpleFeatureType persistedType,
			final SimpleFeatureType reprojectedType,
			final MathTransform transform ) {
		super(
				dataAdapterId,
				composeId(
						STATS_TYPE,
						fieldName));
		this.persistedType = persistedType;
		this.reprojectedType = reprojectedType;
		this.transform = transform;
	}

	public static final ByteArrayId composeId(
			final String fieldName ) {
		return composeId(
				STATS_TYPE,
				fieldName);
	}

	@Override
	public String getFieldName() {
		return decomposeNameFromId(getStatisticsId());
	}

	public Geometry composeGeometry(
			final CoordinateReferenceSystem system ) {
		final Envelope bounds = new Envelope(
				getMinX(),
				getMaxX(),
				getMinY(),
				getMaxY());

		return new GeometryFactory().toGeometry(bounds);
	}

	@Override
	protected Envelope getEnvelope(
			final SimpleFeature entry ) {
		// incorporate the bounding box of the entry's envelope
		final Object o;
		if ((persistedType != null) && (reprojectedType != null) && (transform != null)) {
			o = FeatureDataUtils.defaultCRSTransform(
					entry,
					persistedType,
					reprojectedType,
					transform).getAttribute(
					getFieldName());
		}
		else {
			o = entry.getAttribute(getFieldName());
		}
		if ((o != null) && (o instanceof Geometry)) {
			final Geometry geometry = (Geometry) o;
			if (!geometry.isEmpty()) {
				return geometry.getEnvelopeInternal();
			}
		}
		return null;
	}

	@Override
	public DataStatistics<SimpleFeature> duplicate() {
		return new FeatureBoundingBoxStatistics(
				dataAdapterId,
				getFieldName(),
				persistedType,
				reprojectedType,
				transform);
	}
}
