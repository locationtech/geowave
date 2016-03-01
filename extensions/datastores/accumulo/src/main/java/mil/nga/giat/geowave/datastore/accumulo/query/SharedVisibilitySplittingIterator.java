package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

public class SharedVisibilitySplittingIterator extends
		TransformingIterator
{
	// must execute prior to WholeRowIterator/QueryFilterIterator
	public static final int ITERATOR_PRIORITY = QueryFilterIterator.WHOLE_ROW_ITERATOR_PRIORITY - 1;
	public static final String ITERATOR_NAME = "SHRD_VIZ_SPLT_ITERATOR";

	@Override
	protected PartialKey getKeyPrefix() {
		// this iterator may modify CQ, but will not modify CF
		return PartialKey.ROW_COLFAM;
	}

	@Override
	protected void transformRange(
			final SortedKeyValueIterator<Key, Value> input,
			final KVBuffer output )
			throws IOException {

		while (input.hasTop()) {

			final Key currKey = input.getTopKey();
			final Value currVal = input.getTopValue();

			// check to see if the current value is a composite field containing
			// multiple attributes with shared visibility
			if (Arrays.equals(
					currKey.getColumnQualifierData().getBackingArray(), // fieldId
					AccumuloUtils.COMPOSITE_CQ.getBytes())) {
				// decompose and add transformed <K,V> pairs to output buffer
				for (final Pair<byte[], byte[]> pair : decomposeFlattenedValue(currVal)) {
					final Key newKey = replaceColumnQualifier(
							currKey,
							new Text(
									pair.getLeft()));
					final Value newVal = new Value(
							pair.getRight());
					output.append(
							newKey,
							newVal);
				}
			}

			// simply pass along single (non-composite) value as is
			else {
				output.append(
						currKey,
						currVal);
			}

			input.next();
		}
	}

	private List<Pair<byte[], byte[]>> decomposeFlattenedValue(
			final Value value ) {

		final List<Pair<byte[], byte[]>> retVal = new ArrayList<>();
		final ByteBuffer input = ByteBuffer.wrap(value.get());

		// read the number of attributes combined into this single value
		final int numFields = input.getInt();

		// parse out individual <fieldId, value> pairs
		for (int x = 0; x < numFields; x++) {

			// fieldId
			final int fieldIdLength = input.getInt();
			final byte[] fieldIdBytes = new byte[fieldIdLength];
			input.get(fieldIdBytes);

			// attribute value
			final int fieldLength = input.getInt();
			final byte[] fieldValueBytes = new byte[fieldLength];
			input.get(fieldValueBytes);

			retVal.add(Pair.of(
					fieldIdBytes,
					fieldValueBytes));
		}

		return retVal;
	}
}
