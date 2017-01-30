package mil.nga.giat.geowave.datastore.cassandra;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.DataStoreIndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;

public class CassandraIndexWriter<T> extends
		DataStoreIndexWriter<T, CassandraRow>
{
	public static final Integer PARTITIONS = 4;
	protected final CassandraOperations operations;
	private static long counter = 0;

	public CassandraIndexWriter(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final CassandraOperations operations,
			final IngestCallback<T> callback,
			final Closeable closable ) {
		super(
				adapter,
				index,
				null,
				null,
				callback,
				closable);
		this.operations = operations;
	}

	@Override
	protected void ensureOpen() {
		if (writer == null) {
			writer = operations.createWriter(
					index.getId().getString(),
					true);
		}
	}

	private static <T> List<CassandraRow> getRows(
			final byte[] adapterId,
			final DataStoreEntryInfo ingestInfo,
			final boolean ensureUniqueId ) {
		final List<CassandraRow> rows = new ArrayList<CassandraRow>();
		ByteBuffer allFields = DataStoreUtils.serializeFields(ingestInfo);
		for (final ByteArrayId insertionId : ingestInfo.getInsertionIds()) {
			allFields.rewind();
			byte[] uniqueDataId;
			if (ensureUniqueId) {
				uniqueDataId = DataStoreUtils.ensureUniqueId(
						ingestInfo.getDataId(),
						false).getBytes();
			}
			else {
				uniqueDataId = ingestInfo.getDataId();
			}
			rows.add(new CassandraRow(
					new byte[] {
						(byte) (counter++ % PARTITIONS)
					},
					uniqueDataId,
					adapterId,
					insertionId.getBytes(),
					// TODO: add field mask
					new byte[] {},
					allFields.array()));
		}
		return rows;
	}

	@Override
	protected DataStoreEntryInfo getEntryInfo(
			final T entry,
			final VisibilityWriter<T> visibilityWriter ) {
		final DataStoreEntryInfo entryInfo = DataStoreUtils.getIngestInfo(
				(WritableDataAdapter<T>) adapter,
				index,
				entry,
				DataStoreUtils.UNCONSTRAINED_VISIBILITY);
		if (entryInfo != null) {
			writer.write(getRows(
					adapterId,
					entryInfo,
					(adapter instanceof RowMergingDataAdapter)
							&& (((RowMergingDataAdapter) adapter).getTransform() != null)));
		}
		return entryInfo;
	}

}
