package mil.nga.giat.geowave.core.ingest.hdfs;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.local.AbstractLocalFileDriver;

import org.apache.avro.file.DataFileWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * This class actually executes the staging of data to HDFS based on the
 * available type plugin providers that are discovered through SPI.
 */
public class StageToHdfsDriver extends
		AbstractLocalFileDriver<AvroFormatPlugin<?, ?>, StageRunData>
{
	private final static Logger LOGGER = Logger.getLogger(StageToHdfsDriver.class);
	private HdfsCommandLineOptions hdfsOptions;

	public StageToHdfsDriver(
			final String operation ) {
		super(
				operation);
	}

	@Override
	protected void parseOptionsInternal(
			final CommandLine commandLine )
			throws ParseException {
		hdfsOptions = HdfsCommandLineOptions.parseOptions(commandLine);
		super.parseOptionsInternal(commandLine);
	}

	@Override
	protected void applyOptionsInternal(
			final Options allOptions ) {
		HdfsCommandLineOptions.applyOptions(allOptions);
		super.applyOptionsInternal(allOptions);
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
							"Cannot append data to seequence file",
							e);
				}
			}
		}
	}

	@Override
	protected void runInternal(
			final String[] args,
			final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders ) {

		// first collect the stage to hdfs plugins
		final Map<String, AvroFormatPlugin<?, ?>> stageToHdfsPlugins = new HashMap<String, AvroFormatPlugin<?, ?>>();
		for (final IngestFormatPluginProviderSpi<?, ?> pluginProvider : pluginProviders) {
			AvroFormatPlugin<?, ?> stageToHdfsPlugin = null;
			try {
				stageToHdfsPlugin = pluginProvider.getAvroFormatPlugin();

				if (stageToHdfsPlugin == null) {
					LOGGER.warn("Plugin provider for ingest type '" + pluginProvider.getIngestFormatName() + "' does not support staging to HDFS");
					continue;
				}
			}
			catch (final UnsupportedOperationException e) {
				LOGGER.warn(
						"Plugin provider '" + pluginProvider.getIngestFormatName() + "' does not support staging to HDFS",
						e);
				continue;
			}
			stageToHdfsPlugins.put(
					pluginProvider.getIngestFormatName(),
					stageToHdfsPlugin);
		}
		final Configuration conf = new Configuration();
		conf.set(
				"fs.defaultFS",
				hdfsOptions.getHdfsHostPort());
		conf.set(
				"fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		final Path hdfsBaseDirectory = new Path(
				hdfsOptions.getBasePath());

		try {
			final FileSystem fs = FileSystem.get(conf);
			if (!fs.exists(hdfsBaseDirectory)) {
				fs.mkdirs(hdfsBaseDirectory);
			}
			try {
				final StageRunData runData = new StageRunData(
						hdfsBaseDirectory,
						fs);
				processInput(
						stageToHdfsPlugins,
						runData);
				runData.close();
			}
			catch (final IOException e) {
				LOGGER.fatal(
						"Unexpected I/O exception when reading input files",
						e);
			}
		}
		catch (final IOException e) {
			LOGGER.fatal(
					"Unable to create remote HDFS directory",
					e);
		}
	}
}
