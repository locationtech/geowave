package mil.nga.giat.geowave.ingest.hdfs;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.ingest.IngestTypePluginProviderSpi;
import mil.nga.giat.geowave.ingest.local.AbstractLocalFileDriver;

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
		AbstractLocalFileDriver<StageToHdfsPlugin<?>, StageRunData>
{
	private final static Logger LOGGER = Logger.getLogger(StageToHdfsDriver.class);
	private HdfsCommandLineOptions hdfsOptions;

	public StageToHdfsDriver(
			final String operation ) {
		super(
				operation);
	}

	@Override
	public void parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		hdfsOptions = HdfsCommandLineOptions.parseOptions(commandLine);
		super.parseOptions(commandLine);
	}

	@Override
	public void applyOptions(
			final Options allOptions ) {
		HdfsCommandLineOptions.applyOptions(allOptions);
		super.applyOptions(allOptions);
	}

	@Override
	protected void processFile(
			final File file,
			final String typeName,
			final StageToHdfsPlugin<?> plugin,
			final StageRunData runData ) {
		final DataFileWriter writer = runData.getWriter(
				typeName,
				plugin);
		if (writer != null) {
			final Object[] objs = plugin.toHdfsObjects(file);
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
			final List<IngestTypePluginProviderSpi<?, ?>> pluginProviders ) {

		// first collect the stage to hdfs plugins
		final Map<String, StageToHdfsPlugin<?>> stageToHdfsPlugins = new HashMap<String, StageToHdfsPlugin<?>>();
		for (final IngestTypePluginProviderSpi<?, ?> pluginProvider : pluginProviders) {
			StageToHdfsPlugin<?> stageToHdfsPlugin = null;
			try {
				stageToHdfsPlugin = pluginProvider.getStageToHdfsPlugin();

				if (stageToHdfsPlugin == null) {
					LOGGER.warn("Plugin provider for ingest type '" + pluginProvider.getIngestTypeName() + "' does not support staging to HDFS");
					continue;
				}
			}
			catch (final UnsupportedOperationException e) {
				LOGGER.warn(
						"Plugin provider '" + pluginProvider.getIngestTypeName() + "' does not support staging to HDFS",
						e);
				continue;
			}
			stageToHdfsPlugins.put(
					pluginProvider.getIngestTypeName(),
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
				return;
			}
		}
		catch (final IOException e) {
			LOGGER.fatal(
					"Unable to create remote HDFS directory",
					e);
			return;
		}
	}
}
