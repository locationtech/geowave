package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.data.PersistentValue;

import org.opengis.feature.simple.SimpleFeature;

public class WholeFeatureRowBuilder implements
		RowBuilder<SimpleFeature, Object>
{
	private SimpleFeature feature;

	@Override
	public void setField(
			PersistentValue<Object> fieldValue ) {
		feature = (SimpleFeature) fieldValue.getValue();
	}

	@Override
	public SimpleFeature buildRow(
			ByteArrayId dataId ) {
		return feature;
	}

}
