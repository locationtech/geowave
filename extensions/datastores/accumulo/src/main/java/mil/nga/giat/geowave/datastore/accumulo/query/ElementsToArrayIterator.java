package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.geotime.store.dimension.SpatialField;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.Encoding;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.VariableSizeObjectArrayWriter;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.datastore.accumulo.util.IteratorUtils;
import mil.nga.giat.geowave.datastore.accumulo.util.IteratorUtils.SkeletonKey;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class ElementsToArrayIterator extends
		TransformingIterator
{
	private final static Logger LOGGER = Logger.getLogger(ElementsToArrayIterator.class);

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

			// map field ids to arrays of values
			final Map<ByteArrayId, Map<ByteArrayId, byte[]>> fieldMap = new HashMap<ByteArrayId, Map<ByteArrayId, byte[]>>();
			final Map<ByteArrayId, Byte> encodingMap = new HashMap<ByteArrayId, Byte>();

			final ArrayWriter<Object, byte[]> fixedSizeWriter = new FixedSizeObjectArrayWriter<Object, byte[]>(
					(FieldWriter<Object, byte[]>) FieldUtils.getDefaultWriterForClass(byte[].class));
			final ArrayWriter<Object, byte[]> variableSizeWriter = new VariableSizeObjectArrayWriter<Object, byte[]>(
					(FieldWriter<Object, byte[]>) FieldUtils.getDefaultWriterForClass(byte[].class));

			final List<ByteArrayId> dataIds = new ArrayList<ByteArrayId>();

			final SkeletonKey refKey = new SkeletonKey(
					getSource().getTopKey());

			Key firstKey = null;

			// iterating over simple features
			while (getSource().hasTop()) {

				// only process OUR keys
				if (!refKey.equals(
						getSource().getTopKey(),
						PartialKey.ROW_COLFAM)) {
					break;
				}

				// get all of the key(fieldId) value(field value) pairs for this
				// entry (feature)
				Map<Key, Value> entries = null;
				try {
					entries = WholeRowIterator.decodeRow(
							getSource().getTopKey(),
							getSource().getTopValue());
				}
				catch (final IOException e) {
					return;
				}

				final ByteSequence rowData = getSource().getTopKey().getRowData();
				final AccumuloRowId rowId = new AccumuloRowId(
						rowData.getBackingArray());

				// get the data id as a long
				final ByteArrayId dataId = new ByteArrayId(
						rowId.getDataId());

				dataIds.add(dataId);

				for (final Map.Entry<Key, Value> kvp : entries.entrySet()) {

					if (firstKey == null) {
						firstKey = kvp.getKey();
					}

					// if it's a common index field, we don't want to add the
					// encoding to the value as that would cause issues within
					// the QueryFilterIterator
					final ByteArrayId fieldId = new ByteArrayId(
							kvp.getKey().getColumnQualifierData().getBackingArray());
					final boolean dropEncoding = (model.getReader(fieldId) == null);

					// parse the field value and determine which encoding to use
					byte[] value = entries.get(
							kvp.getKey()).get();
					if (dropEncoding) {
						final ByteBuffer buf = ByteBuffer.wrap(value);
						final Byte encoding = buf.get();

						if (buf.remaining() > 0) {
							final byte[] newValue = new byte[value.length - 1];
							buf.get(newValue);
							value = newValue;
						}
						else {
							value = null;
						}

						// save the encoding if we haven't already
						if (!encodingMap.containsKey(fieldId)) {
							encodingMap.put(
									fieldId,
									encoding);
						}
					}
					else {

						// save the encoding if we haven't already
						if (!encodingMap.containsKey(fieldId)) {
							final DimensionField<?>[] dimFields = model.getDimensions();
							for (final DimensionField<?> dimField : dimFields) {
								if (dimField.getFieldId().equals(
										fieldId)) {
									if (dimField instanceof SpatialField) {
										encodingMap.put(
												fieldId,
												Encoding.VARIABLE_SIZE_ENCODING.getByteEncoding());
										break;
									}
									else if (dimField instanceof TimeField) {
										encodingMap.put(
												fieldId,
												Encoding.FIXED_SIZE_ENCODING.getByteEncoding());
										break;
									}
								}
							}
						}
					}

					// if the field id already exists, add this field value to
					// the map
					if (fieldMap.containsKey(fieldId)) {
						fieldMap.get(
								fieldId).put(
								dataId,
								value);
					}
					// else, create a new map and add this value
					else {
						final Map<ByteArrayId, byte[]> entryMap = new HashMap<ByteArrayId, byte[]>();
						entryMap.put(
								dataId,
								value);
						fieldMap.put(
								fieldId,
								entryMap);
					}
				}

				getSource().next();
			}

			if (firstKey == null) {
				LOGGER.error("Valid firstKey was not found");
			}
			else {
				final ByteSequence rowData = firstKey.getRowData();
				final AccumuloRowId rowId = new AccumuloRowId(
						rowData.getBackingArray());

				// create a new rowid
				final byte[] rowIdBytes = new AccumuloRowId(
						rowId.getInsertionId(),
						new byte[] {},
						rowId.getAdapterId(),
						rowId.getNumberOfDuplicates()).getRowId();

				// set the data id
				final Key rootKey = IteratorUtils.replaceRow(
						replaceColumnQualifier(
								firstKey,
								new Text()),
						rowIdBytes);

				final List<Key> keys = new ArrayList<Key>();
				final List<Value> values = new ArrayList<Value>();

				// now reconstruct the field data arrays
				for (final Map.Entry<ByteArrayId, Map<ByteArrayId, byte[]>> kvp : fieldMap.entrySet()) {
					final Map<ByteArrayId, byte[]> fieldData = kvp.getValue();
					final byte[][] fieldDataBytes = new byte[dataIds.size()][];

					// construct the array of byte arrays
					for (int i = 0; i < dataIds.size(); i++) {
						fieldDataBytes[i] = fieldData.get(dataIds.get(i));
					}

					// use the writer to create a single byte array
					byte[] valueBytes;
					final byte encoding = encodingMap.get(kvp.getKey());
					if (encoding == Encoding.FIXED_SIZE_ENCODING.getByteEncoding()) {
						valueBytes = fixedSizeWriter.writeField(fieldDataBytes);
					}
					else if (encoding == Encoding.VARIABLE_SIZE_ENCODING.getByteEncoding()) {
						valueBytes = variableSizeWriter.writeField(fieldDataBytes);
					}
					else {
						valueBytes = new byte[] {};
					}

					// set the field name
					keys.add(replaceColumnQualifier(
							rootKey,
							new Text(
									kvp.getKey().getBytes())));

					values.add(new Value(
							valueBytes));
				}

				output.append(
						new SkeletonKey(
								rootKey),
						WholeRowIterator.encodeRow(
								keys,
								values));
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
					"Arguments must be set for " + ElementsToArrayIterator.class.getName());
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
