package mil.nga.giat.geowave.core.ingest.hdfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to hold intermediate stage data that must be used throughout the life
 * of the HDFS stage process.
 */
public class StageRunData
{
	private final static Logger LOGGER = LoggerFactory.getLogger(StageRunData.class);
	private final Map<String, DataFileWriter> cachedWriters = new HashMap<String, DataFileWriter>();
	private final Path hdfsBaseDirectory;
	private final FileSystem fs;

	public StageRunData(
			final Path hdfsBaseDirectory,
			final FileSystem fs ) {
		this.hdfsBaseDirectory = hdfsBaseDirectory;
		this.fs = fs;
	}

	public DataFileWriter getWriter(
			final String typeName,
			final AvroFormatPlugin plugin ) {
		return getDataWriterCreateIfNull(
				typeName,
				plugin);
	}

	private synchronized DataFileWriter getDataWriterCreateIfNull(
			final String typeName,
			final AvroFormatPlugin plugin ) {
		if (!cachedWriters.containsKey(typeName)) {
			FSDataOutputStream out = null;
			final DataFileWriter dfw = new DataFileWriter(
					new GenericDatumWriter());
			cachedWriters.put(
					typeName,
					dfw);
			dfw.setCodec(CodecFactory.snappyCodec());
			try {
				// TODO: we should probably clean up the type name to make it
				// HDFS path safe in case there are invalid characters
				// also, if a file already exists do we want to delete it or
				// append to it?
				out = fs.create(new Path(
						hdfsBaseDirectory,
						typeName));
				dfw.create(
						plugin.getAvroSchema(),
						out);

			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to create output stream",
						e);
				// cache a null value so we don't continually try to recreate
				cachedWriters.put(
						typeName,
						null);
				return null;
			}
		}
		return cachedWriters.get(typeName);
	}

	public synchronized void close() {
		for (final DataFileWriter dfw : cachedWriters.values()) {
			try {
				dfw.close();
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to close sequence file stream",
						e);
			}
		}
		cachedWriters.clear();
	}
}
