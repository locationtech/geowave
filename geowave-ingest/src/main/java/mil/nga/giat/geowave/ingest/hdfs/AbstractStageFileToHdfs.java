package mil.nga.giat.geowave.ingest.hdfs;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

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
		byte[] fileBinary;
		try {
			// TODO: consider a streaming mechanism in case a single file is too
			// large
			fileBinary = Files.readAllBytes(f.toPath());
			final ByteBuffer buffer = ByteBuffer.allocate(fileBinary.length);
			buffer.put(fileBinary);
			return new HdfsFile[] {
				new HdfsFile(
						buffer,
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
