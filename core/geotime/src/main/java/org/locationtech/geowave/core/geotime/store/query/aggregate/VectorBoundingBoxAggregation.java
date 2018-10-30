package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

public class VectorBoundingBoxAggregation extends
		BoundingBoxAggregation<FieldNameParam, SimpleFeature>
{
	private FieldNameParam fieldNameParam;

	public VectorBoundingBoxAggregation() {
		this(
				null);
	}

	public VectorBoundingBoxAggregation(
			final FieldNameParam fieldNameParam ) {
		super();
		this.fieldNameParam = fieldNameParam;
	}

	@Override
	public FieldNameParam getParameters() {
		return fieldNameParam;
	}

	@Override
	public void setParameters(
			final FieldNameParam fieldNameParam ) {
		this.fieldNameParam = fieldNameParam;
	}

	@Override
	protected Envelope getEnvelope(
			final SimpleFeature entry ) {
		Object o;
		if ((fieldNameParam != null) && !fieldNameParam.isEmpty()) {
			o = entry.getAttribute(fieldNameParam.getFieldName());
		}
		else {
			o = entry.getDefaultGeometry();
		}
		if ((o != null) && (o instanceof Geometry)) {
			final Geometry geometry = (Geometry) o;
			if (!geometry.isEmpty()) {
				return geometry.getEnvelopeInternal();
			}
		}
		return null;
	}

}
