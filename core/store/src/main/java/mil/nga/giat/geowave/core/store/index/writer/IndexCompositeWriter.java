package mil.nga.giat.geowave.core.store.index.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class IndexCompositeWriter<T> implements
		IndexWriter<T>
{
	final IndexWriter<T>[] writers;

	public IndexCompositeWriter(
			IndexWriter<T>[] writers ) {
		super();
		this.writers = writers;
	}

	@Override
	public void close()
			throws IOException {
		for (IndexWriter<T> indexWriter : writers) {
			indexWriter.close();
		}
	}

	@Override
	public List<ByteArrayId> write(
			T entry )
			throws IOException {
		List<ByteArrayId> ids = new ArrayList<ByteArrayId>();
		for (IndexWriter<T> indexWriter : writers) {
			ids.addAll(indexWriter.write(entry));
		}
		return ids;
	}

	@Override
	public List<ByteArrayId> write(
			T entry,
			VisibilityWriter<T> fieldVisibilityWriter )
			throws IOException {
		List<ByteArrayId> ids = new ArrayList<ByteArrayId>();
		for (IndexWriter<T> indexWriter : writers) {
			ids.addAll(indexWriter.write(
					entry,
					fieldVisibilityWriter));
		}
		return ids;
	}

	@Override
	public PrimaryIndex[] getIndices() {
		List<PrimaryIndex> ids = new ArrayList<PrimaryIndex>();
		for (IndexWriter<T> indexWriter : writers) {
			ids.addAll(Arrays.asList(indexWriter.getIndices()));
		}
		return ids.toArray(new PrimaryIndex[ids.size()]);
	}

	@Override
	public void flush() {
		for (IndexWriter<T> indexWriter : writers) {
			indexWriter.flush();
		}
	}

}
