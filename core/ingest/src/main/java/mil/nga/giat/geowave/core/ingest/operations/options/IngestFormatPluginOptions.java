package mil.nga.giat.geowave.core.ingest.operations.options;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.api.DefaultPluginOptions;
import mil.nga.giat.geowave.core.cli.api.PluginOptions;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestDriver;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatPluginRegistry;

/**
 * This convenience class has methods for loading a list of plugins based on
 * command line options set by the user.
 */
public class IngestFormatPluginOptions extends
		DefaultPluginOptions implements
		PluginOptions
{

	private final static Logger LOGGER = LoggerFactory.getLogger(LocalFileIngestDriver.class);

	private String formats;

	private Map<String, IngestFormatPluginProviderSpi<?, ?>> plugins = new HashMap<String, IngestFormatPluginProviderSpi<?, ?>>();

	@ParametersDelegate
	private Map<String, IngestFormatOptionProvider> options = new HashMap<String, IngestFormatOptionProvider>();

	@Override
	public void selectPlugin(
			String qualifier ) {
		// This is specified as so: format1,format2,...
		formats = qualifier;
		if (qualifier != null && qualifier.length() > 0) {
			for (String name : qualifier.split(",")) {
				addFormat(name.trim());
			}
		}
	}

	private void addFormat(
			String formatName ) {

		IngestFormatPluginProviderSpi<?, ?> formatPlugin = IngestFormatPluginRegistry.getPluginProviderRegistry().get(
				formatName);

		if (formatPlugin == null) {
			throw new ParameterException(
					"Unknown format type specified: " + formatName);
		}

		plugins.put(
				formatName,
				formatPlugin);
		options.put(
				formatName,
				formatPlugin.createOptionsInstances());
	}

	@Override
	public String getType() {
		return formats;
	}

	public Map<String, LocalFileIngestPlugin<?>> createLocalIngestPlugins() {
		Map<String, LocalFileIngestPlugin<?>> ingestPlugins = new HashMap<String, LocalFileIngestPlugin<?>>();
		for (Entry<String, IngestFormatPluginProviderSpi<?, ?>> entry : plugins.entrySet()) {
			IngestFormatPluginProviderSpi<?, ?> formatPlugin = entry.getValue();
			IngestFormatOptionProvider formatOptions = options.get(entry.getKey());
			LocalFileIngestPlugin<?> plugin = null;
			try {
				plugin = formatPlugin.createLocalFileIngestPlugin(formatOptions);
				if (plugin == null) {
					throw new UnsupportedOperationException();
				}
			}
			catch (final UnsupportedOperationException e) {
				LOGGER.warn("Plugin provider for ingest type '" + formatPlugin.getIngestFormatName() + "' does not support local file ingest");
				continue;
			}
			ingestPlugins.put(
					formatPlugin.getIngestFormatName(),
					plugin);
		}
		return ingestPlugins;
	}

	public Map<String, IngestFromHdfsPlugin<?, ?>> createHdfsIngestPlugins() {
		Map<String, IngestFromHdfsPlugin<?, ?>> ingestPlugins = new HashMap<String, IngestFromHdfsPlugin<?, ?>>();
		for (Entry<String, IngestFormatPluginProviderSpi<?, ?>> entry : plugins.entrySet()) {
			IngestFormatPluginProviderSpi<?, ?> formatPlugin = entry.getValue();
			IngestFormatOptionProvider formatOptions = options.get(entry.getKey());
			IngestFromHdfsPlugin<?, ?> plugin = null;
			try {
				plugin = formatPlugin.createIngestFromHdfsPlugin(formatOptions);
				if (plugin == null) {
					throw new UnsupportedOperationException();
				}
			}
			catch (final UnsupportedOperationException e) {
				LOGGER.warn("Plugin provider for ingest type '" + formatPlugin.getIngestFormatName() + "' does not support hdfs ingest");
				continue;
			}
			ingestPlugins.put(
					formatPlugin.getIngestFormatName(),
					plugin);
		}
		return ingestPlugins;
	}

	public Map<String, AvroFormatPlugin<?, ?>> createAvroPlugins() {
		Map<String, AvroFormatPlugin<?, ?>> ingestPlugins = new HashMap<String, AvroFormatPlugin<?, ?>>();
		for (Entry<String, IngestFormatPluginProviderSpi<?, ?>> entry : plugins.entrySet()) {
			IngestFormatPluginProviderSpi<?, ?> formatPlugin = entry.getValue();
			IngestFormatOptionProvider formatOptions = options.get(entry.getKey());
			AvroFormatPlugin<?, ?> plugin = null;
			try {
				plugin = formatPlugin.createAvroFormatPlugin(formatOptions);
				if (plugin == null) {
					throw new UnsupportedOperationException();
				}
			}
			catch (final UnsupportedOperationException e) {
				LOGGER.warn("Plugin provider for ingest type '" + formatPlugin.getIngestFormatName() + "' does not support avro ingest");
				continue;
			}
			ingestPlugins.put(
					formatPlugin.getIngestFormatName(),
					plugin);
		}
		return ingestPlugins;
	}

	public Map<String, IngestFormatPluginProviderSpi<?, ?>> getPlugins() {
		return plugins;
	}

	public void setPlugins(
			Map<String, IngestFormatPluginProviderSpi<?, ?>> plugins ) {
		this.plugins = plugins;
	}

	public Map<String, IngestFormatOptionProvider> getOptions() {
		return options;
	}

	public void setOptions(
			Map<String, IngestFormatOptionProvider> options ) {
		this.options = options;
	}

}
