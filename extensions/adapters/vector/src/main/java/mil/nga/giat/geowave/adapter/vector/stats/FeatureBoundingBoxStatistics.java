package mil.nga.giat.geowave.adapter.vector.stats;

import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

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

	private static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"FEATURE_BBOX");

	private SimpleFeatureType persistedType;
	private SimpleFeatureType reprojectedType;
	private MathTransform transform;

	protected FeatureBoundingBoxStatistics() {
		super();
	}

	public FeatureBoundingBoxStatistics(
			final ByteArrayId dataAdapterId,
			final String statisticsId ) {
		this(
				dataAdapterId,
				statisticsId,
				null,
				null,
				null);
	}

	public FeatureBoundingBoxStatistics(
			final ByteArrayId dataAdapterId,
			final String statisticsId,
			final SimpleFeatureType persistedType,
			final SimpleFeatureType reprojectedType,
			final MathTransform transform ) {
		super(
				dataAdapterId,
				composeId(
						STATS_TYPE.getString(),
						statisticsId));
		this.persistedType = persistedType;
		this.reprojectedType = reprojectedType;
		this.transform = transform;
	}

	public static final ByteArrayId composeId(
			final String fieldName ) {
		return composeId(
				STATS_TYPE.getString(),
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

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(
				"bbox[adapter=").append(
				super.getDataAdapterId().getString());
		buffer.append(
				", field=").append(
				getFieldName());
		if (isSet()) {
			buffer.append(
					", minX=").append(
					minX);
			buffer.append(
					", maxX=").append(
					maxX);
			buffer.append(
					", minY=").append(
					minY);
			buffer.append(
					", maxY=").append(
					maxY);
		}
		else {
			buffer.append(", No Values");
		}
		buffer.append("]");
		return buffer.toString();
	}

	/**
	 * Convert Feature Bounding Box statistics to a JSON object
	 */

	public JSONObject toJSONObject()
			throws JSONException {
		JSONObject jo = new JSONObject();
		jo.put(
				"type",
				STATS_TYPE.getString());
		jo.put(
				"statisticsId",
				statisticsId.getString());

		jo.put(
				"field_identifier",
				getFieldName());

		if (isSet()) {
			jo.put(
					"minX",
					minX);
			jo.put(
					"maxX",
					maxX);
			jo.put(
					"minY",
					minY);
			jo.put(
					"maxY",
					maxY);
		}
		else {
			jo.put(
					"boundaries",
					"No Values");
		}
		return jo;
	}
}
