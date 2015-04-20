package mil.nga.giat.geowave.adapter.vector.merge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.core.store.data.field.ArrayReader;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.Encoding;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import mil.nga.giat.geowave.core.store.data.field.ArrayWriter.VariableSizeObjectArrayWriter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;

public class FeatureCollectionCombiner extends
		Combiner
{

	final private ArrayReader<byte[]> reader = new ArrayReader<byte[]>(
			FieldUtils.getDefaultReaderForClass(byte[].class));

	final private ArrayWriter<?, byte[]> fixedWriter = new FixedSizeObjectArrayWriter<Object, byte[]>(
			(FieldWriter<Object, byte[]>) FieldUtils.getDefaultWriterForClass(byte[].class));

	final private ArrayWriter<?, byte[]> variableWriter = new VariableSizeObjectArrayWriter<Object, byte[]>(
			(FieldWriter<Object, byte[]>) FieldUtils.getDefaultWriterForClass(byte[].class));

	@Override
	public Value reduce(
			final Key key,
			final Iterator<Value> iter ) {
		byte encoding = (byte) -1;
		final List<byte[]> entriesList = new ArrayList<byte[]>();
		while (iter.hasNext()) {
			final byte[] bytes = iter.next().get();
			if (encoding == (byte) -1) {
				encoding = bytes[0];
			}
			final byte[][] entries = reader.readField(bytes);
			entriesList.addAll(Arrays.asList(entries));
		}
		if (!entriesList.isEmpty() && (encoding != (byte) -1)) {
			final ArrayWriter<?, byte[]> writer = (encoding == Encoding.FIXED_SIZE_ENCODING.getByteEncoding()) ? fixedWriter : variableWriter;
			final byte[] outBytes = writer.writeField(entriesList.toArray(new byte[entriesList.size()][]));
			return new Value(
					outBytes);
		}
		return super.getTopValue();
	}
}
