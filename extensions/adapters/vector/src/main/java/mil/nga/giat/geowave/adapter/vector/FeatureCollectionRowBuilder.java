package mil.nga.giat.geowave.adapter.vector;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;

import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * A GeoWave RowBuilder, used internally by AbstractDataAdapter to construct
 * rows from a set field values (in this case SimpleFeatures from a set of
 * attribute values). This implementation simply wraps a geotools
 * SimpleFeatureBuilder.
 * 
 */
public class FeatureCollectionRowBuilder implements
		RowBuilder<DefaultFeatureCollection, Object>
{
	private final List<String> fieldIds;
	private final List<Object[]> fieldData;
	private final SimpleFeatureType type;

	public FeatureCollectionRowBuilder(
			final SimpleFeatureType type ) {
		fieldIds = new ArrayList<String>();
		fieldData = new ArrayList<Object[]>();
		this.type = type;
	}

	@Override
	public DefaultFeatureCollection buildRow(
			final ByteArrayId dataId ) {
		final FieldReader<String[]> reader = FieldUtils.getDefaultReaderForClass(String[].class);

		final String[] dataIds = reader.readField(dataId.getBytes());

		final DefaultFeatureCollection collection = new DefaultFeatureCollection(
				null,
				type);

		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				type);

		for (int i = 0; i < fieldData.get(0).length; i++) {
			for (int j = 0; (j < fieldIds.size()) && (j < fieldData.size()); j++) {
				if ((fieldData.get(j) != null) && (fieldData.get(j)[i] != null)) {
					builder.set(
							fieldIds.get(j),
							fieldData.get(j)[i]);
				}
			}
			collection.add(builder.buildFeature(dataIds[i]));
		}
		return collection;
	}

	@Override
	public void setField(
			final PersistentValue<Object> fieldValue ) {
		fieldIds.add(StringUtils.stringFromBinary(fieldValue.getId().getBytes()));
		fieldData.add((Object[]) fieldValue.getValue());
	}

}
