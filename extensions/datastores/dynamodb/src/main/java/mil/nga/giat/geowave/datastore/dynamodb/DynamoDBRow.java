package mil.nga.giat.geowave.datastore.dynamodb;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Pair;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;

import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValueImpl;
import mil.nga.giat.geowave.datastore.dynamodb.util.DynamoDBUtils;

public class DynamoDBRow implements
		GeoWaveRow
{
	public static final String GW_PARTITION_ID_KEY = "P";
	public static final String GW_RANGE_KEY = "R";
	public static final String GW_FIELD_MASK_KEY = "F";
	public static final String GW_VISIBILITY_KEY = "X";
	public static final String GW_VALUE_KEY = "V";

	private final GeoWaveKey key;
	private final GeoWaveValue[] fieldValues;

	private final List<Map<String, AttributeValue>> objMaps;

	public DynamoDBRow(
			final GeoWaveKey key,
			final List<Map<String, AttributeValue>> objMaps ) {
		this.objMaps = objMaps;

		this.key = key;

		fieldValues = new GeoWaveValueImpl[objMaps.size()];
		int fieldValuesIndex = 0;
		for (final Map<String, AttributeValue> objMap : objMaps) {
			final byte[] fieldMask = objMap.get(
					GW_FIELD_MASK_KEY).getB().array();
			final byte[] value = objMap.get(
					GW_VALUE_KEY).getB().array();
			byte[] visibility = null;
			if (objMap.containsKey(GW_VISIBILITY_KEY)) {
				visibility = objMap.get(
						GW_VISIBILITY_KEY).getB().array();
			}

			fieldValues[fieldValuesIndex] = new GeoWaveValueImpl(
					fieldMask,
					visibility,
					value);
			fieldValuesIndex++;
		}

	}

	public static GeoWaveKey getGeoWaveKey(
			final Map<String, AttributeValue> objMap ) {
		final byte[] partitionKey = objMap.get(
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
				DynamoDBUtils.decodeSortableBase64(sortKey),
				numberOfDuplicates);
	}

	public List<Map<String, AttributeValue>> getAttributeMapping() {
		return objMaps;
	}

	public static class DynamoDBRowMergingIterator implements
			Iterator<DynamoDBRow>
	{

		PeekingIterator<Pair<GeoWaveKey, Map<String, AttributeValue>>> source;

		public DynamoDBRowMergingIterator(final Iterator<Map<String, AttributeValue>> source) {
			final Iterator<Pair<GeoWaveKey, Map<String, AttributeValue>>> iteratorWithKeys =
					Iterators.transform(source, (entry) -> new Pair<>(DynamoDBRow.getGeoWaveKey(entry), entry));
			this.source = Iterators.peekingIterator(iteratorWithKeys);
		}

		@Override
		public boolean hasNext() {
			return source.hasNext();
		}

		@Override
		public DynamoDBRow next() {
			final Pair<GeoWaveKey, Map<String, AttributeValue>> nextValue = source.next();
			final List<Map<String, AttributeValue>> rowValues = Lists.newArrayList();
			rowValues.add(nextValue.getValue());
			while (source.hasNext() && keysEqual(
					nextValue.getKey(),
					source.peek().getKey())) {
				rowValues.add(source.next().getValue());
			}
			return new DynamoDBRow(
					nextValue.getKey(),
					rowValues);
		}

		private boolean keysEqual(
				final GeoWaveKey left,
				final GeoWaveKey right ) {
			return Arrays.equals(
					left.getAdapterId(),
					right.getAdapterId()) && Arrays.equals(
					left.getDataId(),
					right.getDataId()) && Arrays.equals(
					left.getPartitionKey(),
					right.getPartitionKey()) && Arrays.equals(
					left.getSortKey(),
					right.getSortKey()) && (left.getNumberOfDuplicates() == right.getNumberOfDuplicates());
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
			final GeoWaveKey key ) {
		final byte[] sortKey = DynamoDBUtils.encodeSortableBase64(key.getSortKey());
		final ByteBuffer buffer = ByteBuffer.allocate(sortKey.length + key.getAdapterId().length
				+ key.getDataId().length + 20);
		buffer.put(key.getAdapterId());
		buffer.put(sortKey);
		buffer.put(key.getDataId());
		buffer.putLong(Long.MAX_VALUE - System.nanoTime());
		buffer.putInt(key.getAdapterId().length);
		buffer.putInt(key.getDataId().length);
		buffer.putInt(key.getNumberOfDuplicates());
		buffer.rewind();

		return buffer.array();
	}

}
