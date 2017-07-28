package mil.nga.giat.geowave.datastore.dynamodb;

import java.nio.ByteBuffer;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Function;

import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValueImpl;

public class DynamoDBRow implements
		GeoWaveRow
{
	public static final String GW_PARTITION_ID_KEY = "P";
	public static final String GW_RANGE_KEY = "R";
	public static final String GW_FIELD_MASK_KEY = "F";
	public static final String GW_VALUE_KEY = "V";

	private final GeoWaveKey key;
	private final GeoWaveValue[] fieldValues;

	private final Map<String, AttributeValue> objMap;

	public DynamoDBRow(
			final Map<String, AttributeValue> objMap ) {
		this.objMap = objMap;

		this.key = initKey(objMap);

		byte[] fieldMask = objMap.get(
				GW_FIELD_MASK_KEY).getB().array();
		byte[] value = objMap.get(
				GW_VALUE_KEY).getB().array();

		this.fieldValues = new GeoWaveValueImpl[1];
		this.fieldValues[0] = new GeoWaveValueImpl(
				fieldMask,
				null,
				value);
	}

	private GeoWaveKey initKey(
			final Map<String, AttributeValue> objMap ) {
		byte[] partitionKey = objMap.get(
				GW_PARTITION_ID_KEY).getB().array();

		final byte[] rangeKey = objMap.get(
				GW_RANGE_KEY).getB().array();
		final int length = rangeKey.length;

		final ByteBuffer metadataBuf = ByteBuffer.wrap(
				rangeKey,
				length - 12,
				12);
		final int adapterIdLength = metadataBuf.getInt();
		final int dataIdLength = metadataBuf.getInt();
		final int numberOfDuplicates = metadataBuf.getInt();

		final ByteBuffer buf = ByteBuffer.wrap(
				rangeKey,
				0,
				length - 20);
		final byte[] sortKey = new byte[length - 20 - adapterIdLength - dataIdLength];
		final byte[] adapterId = new byte[adapterIdLength];
		final byte[] dataId = new byte[dataIdLength];

		// Range key (row ID) = adapterId + sortKey + dataId
		buf.get(adapterId);
		buf.get(sortKey);
		buf.get(dataId);

		return new GeoWaveKeyImpl(
				dataId,
				adapterId,
				partitionKey,
				sortKey,
				numberOfDuplicates);
	}

	public Map<String, AttributeValue> getAttributeMapping() {
		return objMap;
	}

	public static class GuavaRowTranslationHelper implements
			Function<Map<String, AttributeValue>, DynamoDBRow>
	{
		@Override
		public DynamoDBRow apply(
				final Map<String, AttributeValue> input ) {
			return new DynamoDBRow(
					input);
		}
	}

	@Override
	public byte[] getDataId() {
		return key.getDataId();
	}

	@Override
	public byte[] getAdapterId() {
		return key.getAdapterId();
	}

	@Override
	public byte[] getSortKey() {
		return key.getSortKey();
	}

	@Override
	public byte[] getPartitionKey() {
		return key.getPartitionKey();
	}

	@Override
	public int getNumberOfDuplicates() {
		return key.getNumberOfDuplicates();
	}

	@Override
	public GeoWaveValue[] getFieldValues() {
		return fieldValues;
	}

	public static byte[] getRangeKey(
			GeoWaveKey key ) {
		final ByteBuffer buffer = ByteBuffer.allocate(key.getSortKey().length + key.getAdapterId().length
				+ key.getDataId().length + 20);
		buffer.put(key.getAdapterId());
		buffer.put(key.getSortKey());
		buffer.put(key.getDataId());
		buffer.putLong(System.nanoTime());
		buffer.putInt(key.getAdapterId().length);
		buffer.putInt(key.getDataId().length);
		buffer.putInt(key.getNumberOfDuplicates());
		buffer.rewind();

		return buffer.array();
	}

}
