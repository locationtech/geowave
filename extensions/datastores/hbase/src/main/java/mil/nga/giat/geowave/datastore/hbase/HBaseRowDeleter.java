package mil.nga.giat.geowave.datastore.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.operations.Deleter;
import mil.nga.giat.geowave.datastore.hbase.operations.HBaseWriter;

public class HBaseRowDeleter implements
		Deleter<Object>
{
	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseRowDeleter.class);
	private final HBaseWriter writer;
	private final boolean isAltIndex;

	public HBaseRowDeleter(
			final HBaseWriter writer,
			final boolean isAltIndex ) {
		this.writer = writer;
		this.isAltIndex = isAltIndex;
	}

	@Override
	public void close()
			throws Exception {
		writer.close();
	}

	@Override
	public void delete(
			final DataStoreEntryInfo entry,
			final Object nativeDataStoreEntry,
			final DataAdapter<?> adapter ) {
		final List<Delete> deletes = new ArrayList<Delete>();
		if (isAltIndex) {
			deletes.add(new Delete(
					entry.getDataId()));
		}
		else {
			final List<ByteArrayId> rowIds = entry.getRowIds();
			for (final ByteArrayId id : rowIds) {
				deletes.add(new Delete(
						id.getBytes()));
			}
		}
		try {
			writer.delete(deletes);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to delete rows",
					e);
		}
	}

}
