package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.data.PersistentValue;

import org.opengis.feature.simple.SimpleFeature;

/**
 * A GeoWave RowBuilder, used internally by AbstractDataAdapter to construct
 * rows from a set field values (in this case SimpleFeatures from a set of
 * attribute values). This implementation simply wraps a geotools
 * SimpleFeatureBuilder.
 * 
 */
public class AvroAttributeRowBuilder implements
		RowBuilder<SimpleFeature, Object>
{
	private Object object;

	public AvroAttributeRowBuilder() {}

	@Override
	public SimpleFeature buildRow(
			final ByteArrayId dataId ) {
		SimpleFeature sf = (SimpleFeature) object;
		return sf;
	}

	@Override
	public void setField(
			final PersistentValue<Object> fieldValue ) {
		// Only interested in the relevant fields
		if (fieldValue.getId().equals(
				AvroFeatureAttributeHandler.FIELD_ID)) {
			object = fieldValue.getValue();
		}
	}
}