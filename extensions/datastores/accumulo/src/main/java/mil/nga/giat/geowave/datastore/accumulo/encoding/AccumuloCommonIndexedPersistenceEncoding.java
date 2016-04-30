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

/**
 * Consults adapter to lookup field readers based on bitmasked fieldIds when
 * converting unknown data to adapter extended values
 * 
 * @since 0.9.1
 */
public class AccumuloCommonIndexedPersistenceEncoding extends
		AbstractAdapterPersistenceEncoding
{

	private final List<AccumuloFieldInfo> unknownData;

	public AccumuloCommonIndexedPersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final ByteArrayId indexInsertionId,
			final int duplicateCount,
			final PersistentDataset<CommonIndexValue> commonData,
			final List<AccumuloFieldInfo> unknownData ) {
		super(
				adapterId,
				dataId,
				indexInsertionId,
				duplicateCount,
				commonData,
				new PersistentDataset<byte[]>(),
				new PersistentDataset<Object>());
		this.unknownData = unknownData;

	}

	@Override
	public void convertUnknownValues(
			final DataAdapter<?> adapter,
			final CommonIndexModel model ) {
		if ((unknownData != null) && !unknownData.isEmpty()) {
			for (final AccumuloFieldInfo unknownField : unknownData) {
				final ByteArrayId fieldId = adapter.getFieldIdForPosition(
						model,
						unknownField.getFieldPosition());
				final FieldReader<Object> reader = adapter.getReader(fieldId);
				final Object value = reader.readField(unknownField.getValue());
				adapterExtendedData.addValue(new PersistentValue<Object>(
						fieldId,
						value));
			}
		}
	}

}
