package mil.nga.giat.geowave.accumulo.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import mil.nga.giat.geowave.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.accumulo.util.IteratorUtils;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.store.data.field.ArrayReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.PrimitiveByteArrayReader;
import mil.nga.giat.geowave.store.index.CommonIndexModel;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator;

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

			long longDataId = 0;

			while (input.hasTop()) {
				final Value val = input.getTopValue();
				final Key currentKey = input.getTopKey();

				final ArrayReader<byte[]> reader = new ArrayReader<byte[]>(
						new PrimitiveByteArrayReader());

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
					ByteBuffer buf = ByteBuffer.allocate(8);
					buf.putLong(longDataId++);
					buf.position(4);
					buf.get(
							dataId,
							0,
							4);

					// generate the new rowId
					rowId = new AccumuloRowId(
							rowId.getIndexId(),
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
							buf = ByteBuffer.wrap(value);
							buf.put(val.get()[0]);
							buf.put(entryBytes[i]);
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
		super.init(
				source,
				options,
				env);

		if (options == null) {
			throw new IllegalArgumentException(
					"Arguments must be set for " + ArrayToElementsIterator.class.getName());
		}
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
