/**
 * 
 */
package mil.nga.giat.geowave.core.ingest.hdfs;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.avro.StageToAvroPlugin;
import mil.nga.giat.geowave.core.ingest.local.AbstractLocalHBaseFileDriver;

import org.apache.avro.file.DataFileWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to <code> StageToHdfsDriver </code>
 */
public class StageToHdfsHBaseDriver extends
		AbstractLocalHBaseFileDriver<StageToAvroPlugin<?>, StageRunData>

{
	private final static Logger LOGGER = Logger.getLogger(StageToHdfsHBaseDriver.class);
	private HdfsCommandLineOptions hdfsOptions;

	public StageToHdfsHBaseDriver(
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
			final StageToAvroPlugin<?> plugin,
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
		final Map<String, StageToAvroPlugin<?>> stageToHdfsPlugins = new HashMap<String, StageToAvroPlugin<?>>();
		for (final IngestFormatPluginProviderSpi<?, ?> pluginProvider : pluginProviders) {
			StageToAvroPlugin<?> stageToHdfsPlugin = null;
			try {
				stageToHdfsPlugin = pluginProvider.getStageToAvroPlugin();

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
