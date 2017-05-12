package mil.nga.giat.geowave.datastore.accumulo.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexUtils;

public class AccumuloSecondaryIndexUtils
{

	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloSecondaryIndexUtils.class);

	/**
	 * Decodes an Accumulo key-value pair from the result of a secondary index
	 * scan back into the original type using the given data adapter
	 * 
	 * @param key
	 * @param value
	 * @param adapter
	 * @param indexModel
	 * @return
	 */
	public static <T> T decodeRow(
			final Key key,
			final Value value,
			final DataAdapter<T> adapter,
			final PrimaryIndex index ) {
		Map<Key, Value> rowMapping;
		try {
			rowMapping = WholeRowIterator.decodeRow(
					key,
					value);
		}
		catch (final IOException e) {
			LOGGER.error("Could not decode row from iterator. Ensure whole row iterators are being used.");
			return null;
		}
		final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();
		final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();
		final PersistentDataset<byte[]> unknownData = new PersistentDataset<byte[]>();
		ByteArrayId dataId = null;
		Map<ByteArrayId, byte[]> fieldIdToValueMap = new HashMap<>();
		for (final Entry<Key, Value> entry : rowMapping.entrySet()) {
			final byte[] cqBytes = entry.getKey().getColumnQualifierData().getBackingArray();
			final String dataIdString = SecondaryIndexUtils.getDataId(cqBytes);
			if (dataId == null) {
				dataId = new ByteArrayId(
						dataIdString);
			}
			final ByteArrayId fieldId = SecondaryIndexUtils.getFieldId(cqBytes);
			final byte[] fieldValue = entry.getValue().get();
			fieldIdToValueMap.put(
					fieldId,
					fieldValue);
		}
		for (final Entry<ByteArrayId, byte[]> entry : fieldIdToValueMap.entrySet()) {
			final ByteArrayId fieldId = entry.getKey();
			final byte[] fieldValueBytes = entry.getValue();
			final FieldReader<? extends CommonIndexValue> indexFieldReader = index.getIndexModel().getReader(
					fieldId);
			if (indexFieldReader != null) {
				final CommonIndexValue indexValue = indexFieldReader.readField(fieldValueBytes);
				final PersistentValue<CommonIndexValue> val = new PersistentValue<CommonIndexValue>(
						fieldId,
						indexValue);
				commonData.addValue(val);
			}
			else {
				final FieldReader<?> fieldReader = adapter.getReader(fieldId);
				if (fieldReader != null) {
					final Object fieldValue = fieldReader.readField(fieldValueBytes);
					final PersistentValue<Object> val = new PersistentValue<Object>(
							fieldId,
							fieldValue);
					extendedData.addValue(val);
				}
				else {
					unknownData.addValue(new PersistentValue<byte[]>(
							fieldId,
							fieldValueBytes));
				}
			}
		}
		final IndexedAdapterPersistenceEncoding encodedData = new IndexedAdapterPersistenceEncoding(
				adapter.getAdapterId(),
				dataId,
				null,
				0,
				commonData,
				unknownData,
				extendedData);
		return adapter.decode(
				encodedData,
				index);
	}
}
