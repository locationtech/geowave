package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.data.field.ArrayReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.datastore.accumulo.util.IteratorUtils;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.commons.lang.ArrayUtils;

public class ArrayToElementsIterator extends
		TransformingIterator
{
	public static final String MODEL = "model";

	private CommonIndexModel model;

	@Override
	protected PartialKey getKeyPrefix() {
		return PartialKey.ROW;
	}

	@Override
	protected void transformRange(
			final SortedKeyValueIterator<Key, Value> input,
			final KVBuffer output )
			throws IOException {

		if (model != null) {

			while (input.hasTop()) {
				long longDataId = 0;

				final Value val = input.getTopValue();
				final Key currentKey = input.getTopKey();

				final ArrayReader<byte[]> reader = new ArrayReader<byte[]>(
						FieldUtils.getDefaultReaderForClass(byte[].class));

				// if it's a common index field, we don't want to add the
				// encoding to the value as that would cause issues within
				// the QueryFilterIterator
				final ByteArrayId fieldId = new ByteArrayId(
						currentKey.getColumnQualifierData().getBackingArray());
				final boolean addEncoding = (model.getReader(fieldId) == null);

				final ByteSequence rowData = currentKey.getRowData();
				AccumuloRowId rowId = new AccumuloRowId(
						rowData.getBackingArray());

				// get each entry in the array as arrays of bytes
				final byte[][] entryBytes = reader.readField(val.get());

				// emit each entry as a new key value pair
				for (int i = 0; i < entryBytes.length; i++) {

					// get the data id for this entry
					final byte[] dataId = new byte[4];
					final ByteBuffer buf = ByteBuffer.allocate(8);
					buf.putLong(longDataId++);
					buf.position(4);
					buf.get(
							dataId,
							0,
							4);

					// generate the new rowId
					rowId = new AccumuloRowId(
							rowId.getInsertionId(),
							dataId,
							rowId.getAdapterId(),
							rowId.getNumberOfDuplicates());

					// create a new SkeletonKey with the updated rowId
					final Key outKey = IteratorUtils.replaceRow(
							currentKey,
							rowId.getRowId());

					// append the array encoding to the value so that values can
					// be combined correctly later on
					byte[] value;
					if (addEncoding) {
						if (entryBytes[i] != null) {
							value = new byte[1 + entryBytes[i].length];
							value = ArrayUtils.add(
									entryBytes[i],
									0,
									val.get()[0]);
						}
						else {
							value = new byte[] {
								val.get()[0]
							};
						}
					}
					else {
						value = entryBytes[i];
					}

					output.append(
							outKey,
							new Value(
									value));
				}

				input.next();
			}
		}
	}

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		if (options == null) {
			throw new IllegalArgumentException(
					"Arguments must be set for " + ArrayToElementsIterator.class.getName());
		}
		super.init(
				source,
				options,
				env);
		try {

			final String modelStr = options.get(MODEL);
			final byte[] modelBytes = ByteArrayUtils.byteArrayFromString(modelStr);
			model = PersistenceUtils.fromBinary(
					modelBytes,
					CommonIndexModel.class);
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}
}
