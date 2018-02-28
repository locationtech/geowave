package mil.nga.giat.geowave.datastore.accumulo.operations;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.operations.MetadataType;
import mil.nga.giat.geowave.core.store.operations.MetadataWriter;

public class AccumuloMetadataWriter implements
		MetadataWriter
{
	private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloMetadataWriter.class);
	private final BatchWriter writer;
	private final Text metadataTypeName;

	public AccumuloMetadataWriter(
			final BatchWriter writer,
			final MetadataType metadataType ) {
		this.writer = writer;
		metadataTypeName = getSafeText(metadataType.name());
	}

	@Override
	public void close()
			throws Exception {
		try {
			writer.close();
		}
		catch (final MutationsRejectedException e) {
			LOGGER.warn(
					"Unable to close metadata writer",
					e);
		}
	}

	@Override
	public void write(
			final GeoWaveMetadata metadata ) {
		final Mutation mutation = new Mutation(
				new Text(
						metadata.getPrimaryId()));
		final Text cf = metadataTypeName;
		final Text cq = metadata.getSecondaryId() != null ? new Text(
				metadata.getSecondaryId()) : new Text();
		final byte[] visibility = metadata.getVisibility();
		if (visibility != null) {
			mutation.put(
					cf,
					cq,
					new ColumnVisibility(
							visibility),
					new Value(
							metadata.getValue()));
		}
		else {
			mutation.put(
					cf,
					cq,
					new Value(
							metadata.getValue()));
		}
		try {
			writer.addMutation(mutation);
		}
		catch (final MutationsRejectedException e) {
			LOGGER.error(
					"Unable to write metadata",
					e);
		}
	}

	private static Text getSafeText(
			final String text ) {
		if ((text != null) && !text.isEmpty()) {
			return new Text(
					text);
		}
		else {
			return new Text();
		}
	}

	@Override
	public void flush() {
		try {
			writer.flush();
		}
		catch (final MutationsRejectedException e) {
			LOGGER.warn(
					"Unable to flush metadata writer",
					e);
		}
	}
}
