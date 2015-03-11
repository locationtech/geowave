package mil.nga.giat.geowave.vector.adapter.merge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.store.data.field.ArrayReader;
import mil.nga.giat.geowave.store.data.field.ArrayWriter;
import mil.nga.giat.geowave.store.data.field.ArrayWriter.Encoding;
import mil.nga.giat.geowave.store.data.field.ArrayWriter.FixedSizeObjectArrayWriter;
import mil.nga.giat.geowave.store.data.field.ArrayWriter.VariableSizeObjectArrayWriter;
import mil.nga.giat.geowave.store.data.field.BasicReader.PrimitiveByteArrayReader;
import mil.nga.giat.geowave.store.data.field.BasicWriter.PrimitiveByteArrayWriter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;

public class FeatureCollectionCombiner extends
		Combiner
{

	final private ArrayReader<byte[]> reader = new ArrayReader<byte[]>(
			new PrimitiveByteArrayReader());

	final private ArrayWriter<Object, byte[]> fixedWriter = new FixedSizeObjectArrayWriter<Object, byte[]>(
			new PrimitiveByteArrayWriter());

	final private ArrayWriter<Object, byte[]> variableWriter = new VariableSizeObjectArrayWriter<Object, byte[]>(
			new PrimitiveByteArrayWriter());

	@Override
	public Value reduce(
			final Key key,
			final Iterator<Value> iter ) {
		byte encoding = (byte) -1;
		final List<byte[]> entriesList = new ArrayList<byte[]>();
		while (iter.hasNext()) {
			byte[] bytes = iter.next().get();
			if (encoding == (byte) -1) encoding = bytes[0];
			final byte[][] entries = reader.readField(bytes);
			entriesList.addAll(Arrays.asList(entries));
		}
		if (!entriesList.isEmpty() && encoding != (byte) -1) {
			ArrayWriter<Object, byte[]> writer = (encoding == Encoding.FIXED_SIZE_ENCODING.getByteEncoding()) ? fixedWriter : variableWriter;
			final byte[] outBytes = writer.writeField(entriesList.toArray(new byte[entriesList.size()][]));
			return new Value(
					outBytes);
		}
		return super.getTopValue();
	}
}
