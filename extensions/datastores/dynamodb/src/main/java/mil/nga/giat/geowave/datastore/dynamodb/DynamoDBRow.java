package mil.nga.giat.geowave.datastore.dynamodb;

import java.nio.ByteBuffer;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Function;

import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;

public class DynamoDBRow extends
		GeoWaveKeyImpl
{
	public static final String GW_PARTITION_ID_KEY = "P";
	public static final String GW_RANGE_KEY = "R";
	public static final String GW_FIELD_MASK_KEY = "F";
	public static final String GW_VALUE_KEY = "V";

	private final Map<String, AttributeValue> objMap;
	private String partitionId;

	public DynamoDBRow(
			final String partitionId,
			final byte[] dataId,
			final byte[] adapterId,
			final byte[] index,
			final byte[] fieldMask,
			final byte[] value,
			final int numberOfDuplicates ) {
		super(
				dataId,
				adapterId,
				index,
				fieldMask,
				value,
				numberOfDuplicates);
		this.partitionId = partitionId;
		this.objMap = null; // not needed for ingest
	}

	public DynamoDBRow(
			final Map<String, AttributeValue> objMap ) {
		final byte[] rowId = objMap.get(
				GW_RANGE_KEY).getB().array();
		final int length = rowId.length;
		final int offset = 0;

		final ByteBuffer metadataBuf = ByteBuffer.wrap(
				rowId,
				length + offset - 12,
				12);
		final int adapterIdLength = metadataBuf.getInt();
		final int dataIdLength = metadataBuf.getInt();
		final int numberOfDuplicates = metadataBuf.getInt();

		final ByteBuffer buf = ByteBuffer.wrap(
				rowId,
				offset,
				length - 12);
		final byte[] index = new byte[length - 12 - adapterIdLength - dataIdLength];
		final byte[] adapterId = new byte[adapterIdLength];
		final byte[] dataId = new byte[dataIdLength];
		// get adapterId first
		buf.get(adapterId);
		buf.get(index);
		buf.get(dataId);

		this.dataId = dataId;
		this.adapterId = adapterId;
		this.index = index;
		this.numberOfDuplicates = numberOfDuplicates;

		this.objMap = objMap;

		this.partitionId = objMap.get(
				GW_PARTITION_ID_KEY).getN();

		this.fieldMask = objMap.get(
				GW_FIELD_MASK_KEY).getB().array();

		this.value = objMap.get(
				GW_VALUE_KEY).getB().array();
	}

	@Override
	public byte[] getRowId() {
		final ByteBuffer buf = ByteBuffer.allocate(12 + dataId.length + adapterId.length + index.length);
		buf.put(adapterId);
		buf.put(index);
		buf.put(dataId);
		buf.putInt(adapterId.length);
		buf.putInt(dataId.length);
		buf.putInt(numberOfDuplicates);
		buf.rewind();

		return buf.array();
	}

	public Map<String, AttributeValue> getAttributeMapping() {
		return objMap;
	}

	public String getPartitionId() {
		return partitionId;
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
