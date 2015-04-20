package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.data.PersistentValue;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * A GeoWave RowBuilder, used internally by AbstractDataAdapter to construct
 * rows from a set field values (in this case SimpleFeatures from a set of
 * attribute values). This implementation simply wraps a geotools
 * SimpleFeatureBuilder.
 * 
 */
public class FeatureRowBuilder implements
		RowBuilder<SimpleFeature, Object>
{
	private final SimpleFeatureBuilder builder;

	public FeatureRowBuilder(
			final SimpleFeatureType type ) {
		builder = new SimpleFeatureBuilder(
				type);
	}

	@Override
	public SimpleFeature buildRow(
			final ByteArrayId dataId ) {
		return builder.buildFeature(StringUtils.stringFromBinary(dataId.getBytes()));
	}

	@Override
	public void setField(
			final PersistentValue<Object> fieldValue ) {
		builder.set(
				StringUtils.stringFromBinary(fieldValue.getId().getBytes()),
				fieldValue.getValue());
	}

}
