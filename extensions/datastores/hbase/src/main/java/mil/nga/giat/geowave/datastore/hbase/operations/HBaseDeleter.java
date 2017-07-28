package mil.nga.giat.geowave.datastore.hbase.operations;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.operations.Deleter;

public class HBaseDeleter implements
		Deleter
{
	private static Logger LOGGER = LoggerFactory.getLogger(HBaseDeleter.class);
	private final BufferedMutator deleter;
	protected Set<ByteArrayId> duplicateRowTracker = new HashSet<>();

	public HBaseDeleter(
			final BufferedMutator deleter ) {
		this.deleter = deleter;
	}

	@Override
	public void close() {
		try {
			if (deleter != null) {
				deleter.close();
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to close BufferedMutator",
					e);
		}
	}

	@Override
	public void delete(
			final GeoWaveRow row,
			final DataAdapter<?> adapter ) {

		byte[] rowBytes = GeoWaveKey.getCompositeId(row);
		final Delete delete = new Delete(
				rowBytes);
		// we use a hashset of row IDs so that we can retain multiple versions
		// (otherwise timestamps will be applied on the server side in
		// batches and if the same row exists within a batch we will not
		// retain multiple versions)
		try {
			synchronized (duplicateRowTracker) {
				final ByteArrayId rowId = new ByteArrayId(
						rowBytes);
				if (!duplicateRowTracker.add(rowId)) {
					deleter.flush();
					duplicateRowTracker.clear();
					duplicateRowTracker.add(rowId);
				}
			}
			deleter.mutate(delete);
		}
		catch (IOException e) {
			LOGGER.warn(
					"Unable to delete row",
					e);
		}
	}
}
