package mil.nga.giat.geowave.datastore.accumulo.encoding;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AbstractAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.datastore.accumulo.util.BitmaskUtils;

/**
 * Consults adapter to lookup field readers based on bitmasked fieldIds when
 * converting unknown data to adapter extended values
 * 
 * @since 0.9.1
 */
public class AccumuloCommonIndexedPersistenceEncoding extends
		AbstractAdapterPersistenceEncoding
{

	public AccumuloCommonIndexedPersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final ByteArrayId indexInsertionId,
			final int duplicateCount,
			final PersistentDataset<CommonIndexValue> commonData,
			final PersistentDataset<byte[]> unknownData ) {
		super(
				adapterId,
				dataId,
				indexInsertionId,
				duplicateCount,
				commonData,
				unknownData,
				new PersistentDataset<Object>());
	}

	@Override
	public void convertUnknownValues(
			final DataAdapter<?> adapter,
			final CommonIndexModel model ) {
		final List<PersistentValue<byte[]>> unknownDataValues = getUnknownData().getValues();
		for (final PersistentValue<byte[]> v : unknownDataValues) {
			final byte[] bitmask = v.getId().getBytes();
			final int ordinal = BitmaskUtils.getOrdinal(bitmask);
			final ByteArrayId fieldId = adapter.getFieldIdForPosition(
					model,
					ordinal);
			final FieldReader<Object> reader = adapter.getReader(fieldId);
			final Object value = reader.readField(v.getValue());
			adapterExtendedData.addValue(new PersistentValue<Object>(
					fieldId,
					value));
		}
	}

}
