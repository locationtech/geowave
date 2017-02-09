package mil.nga.giat.geowave.datastore.dynamodb;

import java.nio.ByteBuffer;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Function;

import mil.nga.giat.geowave.core.store.entities.NativeGeoWaveRow;

public class DynamoDBRow implements
		NativeGeoWaveRow
{
	public static final String GW_PARTITION_ID_KEY = "P";
	public static final String GW_RANGE_KEY = "R";
	public static final String GW_FIELD_MASK_KEY = "F";
	public static final String GW_VALUE_KEY = "V";

	private final Map<String, AttributeValue> objMap;
	private byte[] dataId;
	private byte[] idx;
	private byte[] adapterId;

	public DynamoDBRow(
			final Map<String, AttributeValue> objMap ) {
		this.objMap = objMap;
	}

	public Map<String, AttributeValue> getAttributeMapping() {
		return objMap;
	}

	@Override
	public byte[] getDataId() {
		initIds();
		return dataId;
	}

	@Override
	public byte[] getAdapterId() {
		initIds();
		return adapterId;
	}

	@Override
	public byte[] getValue() {
		return objMap.get(
				GW_VALUE_KEY).getB().array();
	}

	@Override
	public byte[] getFieldMask() {
		if (!objMap.containsKey(GW_FIELD_MASK_KEY)) {
			return null;
		}
		return objMap.get(
				GW_FIELD_MASK_KEY).getB().array();
	}

	@Override
	public byte[] getIndex() {
		initIds();
		return idx;
	}

	public synchronized void initIds() {
		if (dataId == null) {
			final ByteBuffer rangeKey = objMap.get(
					GW_RANGE_KEY).getB();
			final int size = rangeKey.remaining();
			rangeKey.position(size - 8);
			final int adapterIdLength = rangeKey.getInt();
			final int dataIdLength = rangeKey.getInt();
			idx = new byte[size - adapterIdLength - dataIdLength - 8];
			adapterId = new byte[adapterIdLength];
			dataId = new byte[dataIdLength];
			rangeKey.rewind();
			rangeKey.get(idx);
			rangeKey.get(adapterId);
			rangeKey.get(dataId);
			rangeKey.rewind();
		}
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
}
