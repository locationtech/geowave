package mil.nga.giat.geowave.core.store.index.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.SinglePartitionInsertionIds;
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
	public InsertionIds write(
			final T entry,
			final VisibilityWriter<T> feldVisibilityWriter ) {
		final Iterator<T> indexedEntries = adapter.convertToIndex(
				index,
				entry);
		final List<SinglePartitionInsertionIds> partitionInsertionIds = new ArrayList<SinglePartitionInsertionIds>();
		while (indexedEntries.hasNext()) {
			InsertionIds ids = writer.write(
					indexedEntries.next(),
					feldVisibilityWriter);
			partitionInsertionIds.addAll(ids.getPartitionKeys());
		}
		return new InsertionIds(
				partitionInsertionIds);

	}

	@Override
	public void close()
			throws IOException {
		writer.close();
	}

	@Override
	public InsertionIds write(
			T entry ) {
		final Iterator<T> indexedEntries = adapter.convertToIndex(
				index,
				entry);
		final List<SinglePartitionInsertionIds> partitionInsertionIds = new ArrayList<SinglePartitionInsertionIds>();
		while (indexedEntries.hasNext()) {
			InsertionIds ids = writer.write(indexedEntries.next());
			partitionInsertionIds.addAll(ids.getPartitionKeys());
		}
		return new InsertionIds(
				partitionInsertionIds);
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