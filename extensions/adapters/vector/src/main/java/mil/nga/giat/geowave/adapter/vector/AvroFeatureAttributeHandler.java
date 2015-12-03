package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;

import org.opengis.feature.simple.SimpleFeature;

/**
 * This is used by the FeatureDataAdapter to handle GeoWave 'fields' using
 * SimpleFeature 'attributes.'
 * 
 */
public class AvroFeatureAttributeHandler implements
		NativeFieldHandler<SimpleFeature, Object>
{
	public static final ByteArrayId FIELD_ID = new ByteArrayId(
			"foo");

	public AvroFeatureAttributeHandler() {}

	@Override
	public ByteArrayId getFieldId() {
		return FIELD_ID;
	}

	@Override
	public Object getFieldValue(
			final SimpleFeature row ) {
		return row;
	}
}