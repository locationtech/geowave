package mil.nga.giat.geowave.ingest.hdfs;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

/**
 * This class can be sub-classed as a general-purpose recipe for parallelizing
 * ingestion of files by directly staging the binary of the file to HDFS.
 */
abstract public class AbstractStageFileToHdfs implements
		StageToHdfsPlugin<HdfsFile>
{
	private final static Logger LOGGER = Logger.getLogger(AbstractStageFileToHdfs.class);

	@Override
	public Schema getAvroSchemaForHdfsType() {
		return HdfsFile.getClassSchema();
	}

	@Override
	public HdfsFile[] toHdfsObjects(
			final File f ) {
		try {
			// TODO: consider a streaming mechanism in case a single file is too
			// large
			return new HdfsFile[] {
				new HdfsFile(
						ByteBuffer.wrap(Files.readAllBytes(f.toPath())),
						f.getAbsolutePath())
			};
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read file",
					e);
		}
		return new HdfsFile[] {};

	}
}
