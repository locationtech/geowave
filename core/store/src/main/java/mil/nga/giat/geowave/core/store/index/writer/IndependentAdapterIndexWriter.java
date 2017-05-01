package mil.nga.giat.geowave.core.store.index.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class IndependentAdapterIndexWriter<T> implements
		IndexWriter<T>
{

	final IndexDependentDataAdapter<T> adapter;
	final PrimaryIndex index;
	final IndexWriter<T> writer;

	public IndependentAdapterIndexWriter(
			IndexDependentDataAdapter<T> adapter,
			PrimaryIndex index,
			IndexWriter<T> writer ) {
		super();
		this.writer = writer;
		this.index = index;
		this.adapter = adapter;
	}

	@Override
	public List<ByteArrayId> write(
			final T entry,
			final VisibilityWriter<T> feldVisibilityWriter )
			throws IOException {
		final Iterator<T> indexedEntries = adapter.convertToIndex(
				index,
				entry);
		final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>();
		while (indexedEntries.hasNext()) {
			rowIds.addAll(writer.write(
					indexedEntries.next(),
					feldVisibilityWriter));
		}
		return rowIds;

	}

	@Override
	public void close()
			throws IOException {
		writer.close();
	}

	@Override
	public List<ByteArrayId> write(
			T entry )
			throws IOException {
		final Iterator<T> indexedEntries = adapter.convertToIndex(
				index,
				entry);
		final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>();
		while (indexedEntries.hasNext()) {
			rowIds.addAll(writer.write(indexedEntries.next()));
		}
		return rowIds;
	}

	@Override
	public PrimaryIndex[] getIndices() {
		return writer.getIndices();
	}

	@Override
	public void flush() {
		writer.flush();
	}
}