package mil.nga.giat.geowave.datastore.cassandra;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
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
	public static final Integer PARTITIONS = 32;
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
					StringUtils.stringFromBinary(
							index.getId().getBytes()),
					true);
		}
	}

	private static <T> List<CassandraRow> getRows(
			final byte[] adapterId,
			final DataStoreEntryInfo ingestInfo ) {
		final List<CassandraRow> rows = new ArrayList<CassandraRow>();
		final List<byte[]> fieldInfoBytesList = new ArrayList<>();
		int totalLength = 0;
		for (final FieldInfo<?> fieldInfo : ingestInfo.getFieldInfo()) {
			final ByteBuffer fieldInfoBytes = ByteBuffer.allocate(
					4 + fieldInfo.getWrittenValue().length);
			fieldInfoBytes.putInt(
					fieldInfo.getWrittenValue().length);
			fieldInfoBytes.put(
					fieldInfo.getWrittenValue());
			fieldInfoBytesList.add(
					fieldInfoBytes.array());
			totalLength += fieldInfoBytes.array().length;
		}
		final ByteBuffer allFields = ByteBuffer.allocate(
				totalLength);
		for (final byte[] bytes : fieldInfoBytesList) {
			allFields.put(
					bytes);
		}
		for (final ByteArrayId insertionId : ingestInfo.getInsertionIds()) {
			final ByteBuffer idBuffer = ByteBuffer.allocate(
					ingestInfo.getDataId().length + adapterId.length + 4);
			idBuffer.putInt(
					ingestInfo.getDataId().length);
			idBuffer.put(
					ingestInfo.getDataId());
			idBuffer.put(
					adapterId);
			idBuffer.rewind();
			allFields.rewind();
			rows.add(
					new CassandraRow(
							new byte[] {
								(byte) (counter++ % PARTITIONS)
							},
							idBuffer.array(),
							insertionId.getBytes(),
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
			writer.write(
					getRows(
							adapterId,
							entryInfo));
		}
		return entryInfo;
	}

}
