package mil.nga.giat.geowave.vector.adapter.merge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.store.data.field.ArrayReader;
import mil.nga.giat.geowave.store.data.field.ArrayWriter;
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

	final private ArrayWriter<Object, byte[]> writer = new ArrayWriter<Object, byte[]>(
			new PrimitiveByteArrayWriter());

	@Override
	public Value reduce(
			final Key key,
			final Iterator<Value> iter ) {
		final List<byte[]> entriesList = new ArrayList<byte[]>();
		while (iter.hasNext()) {
			final byte[][] entries = reader.readField(iter.next().get());
			entriesList.addAll(Arrays.asList(entries));
		}
		if (!entriesList.isEmpty()) {
			final byte[] outBytes = writer.writeField(entriesList.toArray(new byte[entriesList.size()][]));
			return new Value(
					outBytes);
		}
		return super.getTopValue();
	}
}
