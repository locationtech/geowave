package mil.nga.giat.geowave.core.store.index.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.SinglePartitionInsertionIds;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class IndexCompositeWriter<T> implements
		IndexWriter<T>
{
	final IndexWriter<T>[] writers;

	public IndexCompositeWriter(
			final IndexWriter<T>[] writers ) {
		super();
		this.writers = writers;
	}

	@Override
	public void close()
			throws IOException {
		for (final IndexWriter<T> indexWriter : writers) {
			indexWriter.close();
		}
	}

	@Override
	public InsertionIds write(
			final T entry ) {
		final List<SinglePartitionInsertionIds> ids = new ArrayList<SinglePartitionInsertionIds>();

		for (final IndexWriter<T> indexWriter : writers) {
			final InsertionIds i = indexWriter.write(entry);
			ids.addAll(i.getPartitionKeys());
		}
		return new InsertionIds(
				ids);
	}

	@Override
	public InsertionIds write(
			final T entry,
			final VisibilityWriter<T> fieldVisibilityWriter ) {
		final List<SinglePartitionInsertionIds> ids = new ArrayList<SinglePartitionInsertionIds>();
		for (final IndexWriter<T> indexWriter : writers) {
			final InsertionIds i = indexWriter.write(
					entry,
					fieldVisibilityWriter);
			ids.addAll(i.getPartitionKeys());
		}
		return new InsertionIds(
				ids);
	}

	@Override
	public PrimaryIndex[] getIndices() {
		final List<PrimaryIndex> ids = new ArrayList<PrimaryIndex>();
		for (final IndexWriter<T> indexWriter : writers) {
			ids.addAll(Arrays.asList(indexWriter.getIndices()));
		}
		return ids.toArray(new PrimaryIndex[ids.size()]);
	}

	@Override
	public void flush() {
		for (final IndexWriter<T> indexWriter : writers) {
			indexWriter.flush();
		}
	}

}
