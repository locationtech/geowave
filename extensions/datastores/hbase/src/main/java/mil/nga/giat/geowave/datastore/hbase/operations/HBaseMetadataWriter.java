package mil.nga.giat.geowave.datastore.hbase.operations;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.operations.MetadataType;
import mil.nga.giat.geowave.core.store.operations.MetadataWriter;

public class HBaseMetadataWriter implements
		MetadataWriter
{
	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseMetadataWriter.class);

	private final BufferedMutator writer;
	protected Set<ByteArrayId> duplicateRowTracker = new HashSet<>();
	private final byte[] metadataTypeBytes;

	public HBaseMetadataWriter(
			final BufferedMutator writer,
			final MetadataType metadataType ) {
		this.writer = writer;
		metadataTypeBytes = StringUtils.stringToBinary(metadataType.name());
	}

	@Override
	public void close()
			throws Exception {
		try {
			writer.close();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to close metadata writer",
					e);
		}
	}

	@Override
	public void write(
			final GeoWaveMetadata metadata ) {

		// we use a hashset of row IDs so that we can retain multiple versions
		// (otherwise timestamps will be applied on the server side in
		// batches and if the same row exists within a batch we will not
		// retain multiple versions)
		final Put put = new Put(
				metadata.getPrimaryId());

		final byte[] secondaryBytes = metadata.getSecondaryId() != null ? metadata.getSecondaryId() : new byte[0];

		put.addColumn(
				metadataTypeBytes,
				secondaryBytes,
				metadata.getValue());

		if (metadata.getVisibility() != null) {
			put.setCellVisibility(new CellVisibility(
					StringUtils.stringFromBinary(metadata.getVisibility())));
		}

		try {
			synchronized (duplicateRowTracker) {
				final ByteArrayId primaryId = new ByteArrayId(
						metadata.getPrimaryId());
				if (!duplicateRowTracker.add(primaryId)) {
					writer.flush();
					duplicateRowTracker.clear();
					duplicateRowTracker.add(primaryId);
				}
			}
			writer.mutate(put);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to write metadata",
					e);
		}

	}

	@Override
	public void flush() {
		try {
			synchronized (duplicateRowTracker) {
				writer.flush();
				duplicateRowTracker.clear();
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to flush metadata writer",
					e);
		}
	}
}
