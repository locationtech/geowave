package mil.nga.giat.geowave.core.ingest.hdfs;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.local.AbstractLocalFileDriver;
import mil.nga.giat.geowave.core.ingest.local.LocalInputCommandLineOptions;

/**
 * This class actually executes the staging of data to HDFS based on the
 * available type plugin providers that are discovered through SPI.
 */
public class StageToHdfsDriver extends
		AbstractLocalFileDriver<AvroFormatPlugin<?, ?>, StageRunData>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(StageToHdfsDriver.class);
	private final Map<String, AvroFormatPlugin<?, ?>> ingestPlugins;
	private final String hdfsHostPort;
	private final String basePath;

	public StageToHdfsDriver(
			Map<String, AvroFormatPlugin<?, ?>> ingestPlugins,
			String hdfsHostPort,
			String basePath,
			LocalInputCommandLineOptions inputOptions ) {
		super(
				inputOptions);
		this.ingestPlugins = ingestPlugins;
		this.hdfsHostPort = hdfsHostPort;
		this.basePath = basePath;
	}

	@Override
	protected void processFile(
			final File file,
			final String typeName,
			final AvroFormatPlugin<?, ?> plugin,
			final StageRunData runData ) {
		final DataFileWriter writer = runData.getWriter(
				typeName,
				plugin);
		if (writer != null) {
			final Object[] objs = plugin.toAvroObjects(file);
			for (final Object obj : objs) {
				try {
					writer.append(obj);
				}
				catch (final IOException e) {
					LOGGER.error(
							"Cannot append data to sequence file",
							e);
				}
			}
		}
	}

	public boolean runOperation(
			String inputPath ) {

		// first collect the stage to hdfs plugins
		final Map<String, AvroFormatPlugin<?, ?>> stageToHdfsPlugins = ingestPlugins;
		final Configuration conf = new Configuration();
		conf.set(
				"fs.defaultFS",
				hdfsHostPort);
		conf.set(
				"fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		final Path hdfsBaseDirectory = new Path(
				basePath);

		try {
			try (final FileSystem fs = FileSystem.get(conf)) {
				if (!fs.exists(hdfsBaseDirectory)) {
					fs.mkdirs(hdfsBaseDirectory);
				}
				try {
					final StageRunData runData = new StageRunData(
							hdfsBaseDirectory,
							fs);
					processInput(
							inputPath,
							stageToHdfsPlugins,
							runData);
					runData.close();
					return true;
				}
				catch (final IOException e) {
					LOGGER.error(
							"Unexpected I/O exception when reading input files",
							e);
					return false;
				}
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to create remote HDFS directory",
					e);
			return false;
		}
	}
}
