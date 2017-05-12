package mil.nga.giat.geowave.adapter.vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.core.store.data.field.FieldReader;

public class AvroFeatureReader implements
		FieldReader<Object>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AvroFeatureReader.class);

	@Override
	public Object readField(
			final byte[] fieldData ) {
		SimpleFeature deserializedSimpleFeature = null;
		try {
			deserializedSimpleFeature = AvroFeatureUtils.deserializeAvroSimpleFeature(fieldData);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to deserialize SimpleFeature",
					e);
		}

		return deserializedSimpleFeature;
	}

}