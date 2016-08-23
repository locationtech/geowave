package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;

public class AttributeSubsettingIterator extends
		TransformingIterator
{
	private static final int ITERATOR_PRIORITY = QueryFilterIterator.QUERY_ITERATOR_PRIORITY + 1;
	private static final String ITERATOR_NAME = "ATTRIBUTE_SUBSETTING_ITERATOR";

	private static final String FIELD_SUBSET_BITMASK = "fieldsBitmask";
	public static final String WHOLE_ROW_ENCODED_KEY = "wholerow";
	private byte[] fieldSubsetBitmask;
	private boolean wholeRowEncoded;

	@Override
	protected PartialKey getKeyPrefix() {
		return PartialKey.ROW;
	}

	@Override
	protected void transformRange(
			final SortedKeyValueIterator<Key, Value> input,
			final KVBuffer output )
			throws IOException {
		while (input.hasTop()) {
			final Key wholeRowKey = input.getTopKey();
			final Value wholeRowVal = input.getTopValue();
			final SortedMap<Key, Value> rowMapping;
			if (wholeRowEncoded) {
				rowMapping = WholeRowIterator.decodeRow(
						wholeRowKey,
						wholeRowVal);
			}
			else {
				rowMapping = new TreeMap<Key, Value>();
				rowMapping.put(
						wholeRowKey,
						wholeRowVal);
			}
			final List<Key> keyList = new ArrayList<>();
			final List<Value> valList = new ArrayList<>();
			Text adapterId = null;

			for (final Entry<Key, Value> row : rowMapping.entrySet()) {
				final Key currKey = row.getKey();
				final Value currVal = row.getValue();
				if (adapterId == null) {
					adapterId = currKey.getColumnFamily();
				}
				final byte[] originalBitmask = currKey.getColumnQualifierData().getBackingArray();
				final byte[] newBitmask = BitmaskUtils.generateANDBitmask(
						originalBitmask,
						fieldSubsetBitmask);
				if (BitmaskUtils.isAnyBitSet(newBitmask)) {
					if (!Arrays.equals(
							newBitmask,
							originalBitmask)) {
						keyList.add(replaceColumnQualifier(
								currKey,
								new Text(
										newBitmask)));
						valList.add(constructNewValue(
								currVal,
								originalBitmask,
								newBitmask));
					}
					else {
						// pass along unmodified
						keyList.add(currKey);
						valList.add(currVal);
					}
				}
			}
			if (!keyList.isEmpty() && !valList.isEmpty()) {
				final Value outputVal;
				final Key outputKey;
				if (wholeRowEncoded) {
					outputKey = new Key(
							wholeRowKey.getRow(),
							adapterId);
					outputVal = WholeRowIterator.encodeRow(
							keyList,
							valList);
				}
				else {
					outputKey = keyList.get(0);
					outputVal = valList.get(0);
				}
				output.append(
						outputKey,
						outputVal);
			}
			input.next();
		}
	}

	private Value constructNewValue(
			final Value original,
			final byte[] originalBitmask,
			final byte[] newBitmask ) {
		final ByteBuffer originalBytes = ByteBuffer.wrap(original.get());
		final List<byte[]> valsToKeep = new ArrayList<>();
		int totalSize = 0;
		final List<Integer> originalPositions = BitmaskUtils.getFieldPositions(originalBitmask);
		// convert list to set for quick contains()
		final Set<Integer> newPositions = new HashSet<Integer>(
				BitmaskUtils.getFieldPositions(newBitmask));
		if (originalPositions.size() > 1) {
			for (final Integer originalPosition : originalPositions) {
				final int len = originalBytes.getInt();
				final byte[] val = new byte[len];
				originalBytes.get(val);
				if (newPositions.contains(originalPosition)) {
					valsToKeep.add(val);
					totalSize += len;
				}
			}
		}
		else if (!newPositions.isEmpty()) {
			// this shouldn't happen because we should already catch the case
			// where the bitmask is unchanged
			return original;
		}
		else {
			// and this shouldn't happen because we should already catch the
			// case where the resultant bitmask is empty
			return null;
		}
		if (valsToKeep.size() == 1) {
			final ByteBuffer retVal = ByteBuffer.allocate(totalSize);
			retVal.put(valsToKeep.get(0));
			return new Value(
					retVal.array());
		}
		final ByteBuffer retVal = ByteBuffer.allocate((valsToKeep.size() * 4) + totalSize);
		for (final byte[] val : valsToKeep) {
			retVal.putInt(val.length);
			retVal.put(val);
		}
		return new Value(
				retVal.array());
	}

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		super.init(
				source,
				options,
				env);
		// get fieldIds and associated adapter
		final String bitmaskStr = options.get(FIELD_SUBSET_BITMASK);
		fieldSubsetBitmask = ByteArrayUtils.byteArrayFromString(bitmaskStr);
		final String wholeRowEncodedStr = options.get(WHOLE_ROW_ENCODED_KEY);
		// default to whole row encoded if not specified
		wholeRowEncoded = (wholeRowEncodedStr == null || !wholeRowEncodedStr.equals(Boolean.toString(false)));
	}

	@Override
	public boolean validateOptions(
			final Map<String, String> options ) {
		if ((!super.validateOptions(options)) || (options == null)) {
			return false;
		}
		final boolean hasFieldsBitmask = options.containsKey(FIELD_SUBSET_BITMASK);
		if (!hasFieldsBitmask) {
			// all are required
			return false;
		}
		return true;
	}

	/**
	 * 
	 * @return an {@link IteratorSetting} for this iterator
	 */
	public static IteratorSetting getIteratorSetting() {
		return new IteratorSetting(
				AttributeSubsettingIterator.ITERATOR_PRIORITY,
				AttributeSubsettingIterator.ITERATOR_NAME,
				AttributeSubsettingIterator.class);
	}

	/**
	 * Sets the desired subset of fields to keep
	 * 
	 * @param setting
	 *            the {@link IteratorSetting}
	 * @param adapterAssociatedWithFieldIds
	 *            the adapter associated with the given fieldIds
	 * @param fieldIds
	 *            the desired subset of fieldIds
	 * @param numericDimensions
	 *            the numeric dimension fields
	 */
	public static void setFieldIds(
			final IteratorSetting setting,
			final DataAdapter<?> adapterAssociatedWithFieldIds,
			final List<String> fieldIds,
			final CommonIndexModel indexModel ) {
		final SortedSet<Integer> fieldPositions = new TreeSet<Integer>();

		// dimension fields must also be included
		for (final NumericDimensionField<? extends CommonIndexValue> dimension : indexModel.getDimensions()) {
			fieldPositions.add(adapterAssociatedWithFieldIds.getPositionOfOrderedField(
					indexModel,
					dimension.getFieldId()));
		}

		for (final String fieldId : fieldIds) {
			fieldPositions.add(adapterAssociatedWithFieldIds.getPositionOfOrderedField(
					indexModel,
					new ByteArrayId(
							fieldId)));
		}
		final byte[] fieldSubsetBitmask = BitmaskUtils.generateCompositeBitmask(fieldPositions);

		setting.addOption(
				FIELD_SUBSET_BITMASK,
				ByteArrayUtils.byteArrayToString(fieldSubsetBitmask));
	}
}
