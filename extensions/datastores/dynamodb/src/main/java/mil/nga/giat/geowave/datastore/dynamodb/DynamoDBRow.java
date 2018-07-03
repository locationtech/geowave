package mil.nga.giat.geowave.datastore.dynamodb;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.beust.jcommander.internal.Lists;
import com.google.common.base.Function;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValueImpl;
import mil.nga.giat.geowave.core.store.entities.MergeableGeoWaveRow;
import mil.nga.giat.geowave.datastore.dynamodb.util.DynamoDBUtils;

public class DynamoDBRow extends
		MergeableGeoWaveRow implements
		GeoWaveRow
{
	public static final String GW_PARTITION_ID_KEY = "P";
	public static final String GW_RANGE_KEY = "R";
	public static final String GW_FIELD_MASK_KEY = "F";
	public static final String GW_VISIBILITY_KEY = "X";
	public static final String GW_VALUE_KEY = "V";

	private final GeoWaveKey key;

	private final List<Map<String, AttributeValue>> objMaps = Lists.newArrayList();

	public DynamoDBRow(
			final Map<String, AttributeValue> objMap ) {
		super(
				getFieldValues(objMap));

		this.objMaps.add(objMap);
		this.key = getGeoWaveKey(objMap);
	}

	private static GeoWaveValue[] getFieldValues(
			Map<String, AttributeValue> objMap ) {
		final GeoWaveValue[] fieldValues = new GeoWaveValueImpl[1];
		final byte[] fieldMask = objMap.get(
				GW_FIELD_MASK_KEY).getB().array();
		final byte[] value = objMap.get(
				GW_VALUE_KEY).getB().array();
		byte[] visibility = null;
		if (objMap.containsKey(GW_VISIBILITY_KEY)) {
			visibility = objMap.get(
					GW_VISIBILITY_KEY).getB().array();
		}

		fieldValues[0] = new GeoWaveValueImpl(
				fieldMask,
				visibility,
				value);
		return fieldValues;
	}

	private static GeoWaveKey getGeoWaveKey(
			final Map<String, AttributeValue> objMap ) {
		final byte[] partitionKey = objMap.get(
				GW_PARTITION_ID_KEY).getB().array();

		final byte[] rangeKey = objMap.get(
				GW_RANGE_KEY).getB().array();
		final int length = rangeKey.length;

		final ByteBuffer metadataBuf = ByteBuffer.wrap(
				rangeKey,
				length - 8,
				8);
		final int dataIdLength = metadataBuf.getInt();
		final int numberOfDuplicates = metadataBuf.getInt();

		final ByteBuffer buf = ByteBuffer.wrap(
				rangeKey,
				0,
				length - 16);
		final byte[] sortKey = new byte[length - 16 - 2 - dataIdLength];
		final byte[] dataId = new byte[dataIdLength];

		// Range key (row ID) = adapterId + sortKey + dataId
		byte[] internalAdapterIdBytes = new byte[2];
		buf.get(internalAdapterIdBytes);
		short internalAdapterId = ByteArrayUtils.byteArrayToShort(internalAdapterIdBytes);
		buf.get(sortKey);
		buf.get(dataId);

		return new GeoWaveKeyImpl(
				dataId,
				internalAdapterId,
				partitionKey,
				DynamoDBUtils.decodeSortableBase64(sortKey),
				numberOfDuplicates);
	}

	public List<Map<String, AttributeValue>> getAttributeMapping() {
		return objMaps;
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
	public short getInternalAdapterId() {
		return key.getInternalAdapterId();
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
	public void mergeRowInternal(
			MergeableGeoWaveRow row ) {
		if (row instanceof DynamoDBRow) {
			objMaps.addAll(((DynamoDBRow) row).getAttributeMapping());
		}
	}

	public static byte[] getRangeKey(
			final GeoWaveKey key ) {
		final byte[] sortKey = DynamoDBUtils.encodeSortableBase64(key.getSortKey());
		final ByteBuffer buffer = ByteBuffer.allocate(sortKey.length + key.getDataId().length + 18);
		buffer.put(ByteArrayUtils.shortToByteArray(key.getInternalAdapterId()));
		buffer.put(sortKey);
		buffer.put(key.getDataId());
		buffer.putLong(Long.MAX_VALUE - System.nanoTime());
		buffer.putInt(key.getDataId().length);
		buffer.putInt(key.getNumberOfDuplicates());
		buffer.rewind();

		return buffer.array();
	}

}
