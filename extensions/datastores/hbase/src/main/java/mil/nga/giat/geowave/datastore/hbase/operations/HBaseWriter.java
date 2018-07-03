package mil.nga.giat.geowave.datastore.hbase.operations;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.operations.Writer;

/**
 * This is a basic wrapper around the HBase BufferedMutator so that write
 * operations will use an interface that can be implemented differently for
 * different purposes. For example, a bulk ingest can be performed by replacing
 * this implementation within a custom implementation of HBaseOperations.
 */
public class HBaseWriter implements
		Writer
{
	private final static Logger LOGGER = Logger.getLogger(HBaseWriter.class);

	protected Set<ByteArrayId> duplicateRowTracker = new HashSet<>();
	private final BufferedMutator mutator;
	private final HBaseOperations operations;
	private final String tableName;

	public HBaseWriter(
			final BufferedMutator mutator,
			final HBaseOperations operations,
			final String tableName ) {
		this.mutator = mutator;
		this.operations = operations;
		this.tableName = tableName;
	}

	@Override
	public void close() {
		try {
			mutator.close();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to close BufferedMutator",
					e);
		}
	}

	@Override
	public void flush() {
		try {
			synchronized (duplicateRowTracker) {
				mutator.flush();
				duplicateRowTracker.clear();
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to flush BufferedMutator",
					e);
		}
	}

	@Override
	public void write(
			final GeoWaveRow[] rows ) {
		for (final GeoWaveRow row : rows) {
			write(row);
		}
	}

	@Override
	public void write(
			final GeoWaveRow row ) {
		final byte[] partition = row.getPartitionKey();
		if ((partition != null) && (partition.length > 0)) {
			operations.insurePartition(
					new ByteArrayId(
							partition),
					tableName);
		}

		writeMutations(rowToMutation(row));
	}

	private void writeMutations(
			final RowMutations rowMutation ) {
		try {
			mutator.mutate(rowMutation.getMutations());
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to write mutation.",
					e);
		}
	}

	private RowMutations rowToMutation(
			final GeoWaveRow row ) {
		final byte[] rowBytes = GeoWaveKey.getCompositeId(row);

		// we use a hashset of row IDs so that we can retain multiple versions
		// (otherwise timestamps will be applied on the server side in
		// batches and if the same row exists within a batch we will not
		// retain multiple versions)
		synchronized (duplicateRowTracker) {
			final ByteArrayId rowId = new ByteArrayId(
					rowBytes);
			if (!duplicateRowTracker.add(rowId)) {
				try {
					mutator.flush();
					duplicateRowTracker.clear();
					duplicateRowTracker.add(rowId);
				}
				catch (final IOException e) {
					LOGGER.error(
							"Unable to write mutation.",
							e);
				}
			}
		}

		final RowMutations mutation = new RowMutations(
				rowBytes);
		for (final GeoWaveValue value : row.getFieldValues()) {
			final Put put = new Put(
					rowBytes);

			put.addColumn(
					StringUtils.stringToBinary(ByteArrayUtils.shortToString(row.getInternalAdapterId())),
					value.getFieldMask(),
					value.getValue());

			if ((value.getVisibility() != null) && (value.getVisibility().length > 0)) {
				put.setCellVisibility(new CellVisibility(
						StringUtils.stringFromBinary(value.getVisibility())));
			}

			try {
				mutation.add(put);
			}
			catch (final IOException e) {
				LOGGER.error("Error creating HBase row mutation: " + e.getMessage());
			}
		}

		return mutation;
	}
}
