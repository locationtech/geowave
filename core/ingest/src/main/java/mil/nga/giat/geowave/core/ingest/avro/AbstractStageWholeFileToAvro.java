package mil.nga.giat.geowave.core.ingest.avro;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class can be sub-classed as a general-purpose recipe for parallelizing
 * ingestion of files by directly staging the binary of the file to Avro.
 */
abstract public class AbstractStageWholeFileToAvro<O> implements
		AvroFormatPlugin<WholeFile, O>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractStageWholeFileToAvro.class);

	@Override
	public Schema getAvroSchema() {
		return WholeFile.getClassSchema();
	}

	@Override
	public WholeFile[] toAvroObjects(
			final File f ) {
		try {
			// TODO: consider a streaming mechanism in case a single file is too
			// large
			return new WholeFile[] {
				new WholeFile(
						ByteBuffer.wrap(Files.readAllBytes(f.toPath())),
						f.getAbsolutePath())
			};
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read file",
					e);
		}
		return new WholeFile[] {};

	}
}
