package mil.nga.giat.geowave.core.store.adapter;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

/**
 * This is an implements of persistence encoding that also contains all of the
 * extended data values used to form the native type supported by this adapter.
 * It also contains information about the persisted object within a particular
 * index such as the insertion ID in the index and the number of duplicates for
 * this entry in the index, and is used when reading data from the index.
 */
public class IndexedAdapterPersistenceEncoding extends
		AbstractAdapterPersistenceEncoding
{
	public IndexedAdapterPersistenceEncoding(
			final ByteArrayId adapterId,
			final ByteArrayId dataId,
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey,
			final int duplicateCount,
			final PersistentDataset<CommonIndexValue> commonData,
			final PersistentDataset<byte[]> unknownData,
			final PersistentDataset<Object> adapterExtendedData ) {
		super(
				adapterId,
				dataId,
				partitionKey,
				sortKey,
				duplicateCount,
				commonData,
				unknownData,
				adapterExtendedData);
	}

	@Override
	public void convertUnknownValues(
			final DataAdapter<?> adapter,
			final CommonIndexModel model ) {
		final List<PersistentValue<byte[]>> unknownDataValues = getUnknownData().getValues();
		for (final PersistentValue<byte[]> v : unknownDataValues) {
			final FieldReader<Object> reader = adapter.getReader(v.getId());
			final Object value = reader.readField(v.getValue());
			adapterExtendedData.addValue(new PersistentValue<Object>(
					v.getId(),
					value));
		}
	}
}