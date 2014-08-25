package mil.nga.giat.geowave.vector.adapter;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.store.data.PersistentValue;
import mil.nga.giat.geowave.store.data.field.ArrayReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.StringReader;

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
		final ArrayReader<String> reader = new ArrayReader<String>(
				new StringReader());

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
			collection.add(builder.buildFeature(null));
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
