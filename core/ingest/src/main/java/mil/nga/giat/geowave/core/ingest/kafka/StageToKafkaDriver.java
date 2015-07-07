package mil.nga.giat.geowave.core.ingest.kafka;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.local.AbstractLocalFileDriver;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * This class actually executes the staging of data to a Kafka topic based on
 * the available type plugin providers that are discovered through SPI.
 */
public class StageToKafkaDriver<T extends SpecificRecordBase> extends
		AbstractLocalFileDriver<AvroFormatPlugin<?, ?>, StageKafkaData<?>>
{
	private final static Logger LOGGER = Logger.getLogger(StageToKafkaDriver.class);
	private KafkaProducerCommandLineOptions kafkaOptions;

	public StageToKafkaDriver(
			final String operation ) {
		super(
				operation);
	}

	@Override
	protected void parseOptionsInternal(
			final CommandLine commandLine )
			throws ParseException {
		kafkaOptions = KafkaProducerCommandLineOptions.parseOptions(commandLine);
		super.parseOptionsInternal(commandLine);
	}

	@Override
	protected void applyOptionsInternal(
			final Options allOptions ) {
		KafkaProducerCommandLineOptions.applyOptions(allOptions);
		super.applyOptionsInternal(allOptions);

	}

	@Override
	protected void processFile(
			final File file,
			final String typeName,
			final AvroFormatPlugin<?, ?> plugin,
			final StageKafkaData<?> runData ) {

		try {
			final Producer<String, Object> producer = (Producer<String, Object>) runData.getProducer(
					typeName,
					plugin);
			final Object[] avroRecords = plugin.toAvroObjects(file);
			for (final Object avroRecord : avroRecords) {
				final KeyedMessage<String, Object> data = new KeyedMessage<String, Object>(
						typeName,
						avroRecord);
				producer.send(data);
			}
		}
		catch (final Exception e) {
			LOGGER.info("Unable to send file [" + file.getAbsolutePath() + "] to Kafka topic: " + e.getMessage());
		}
	}

	@Override
	protected void runInternal(
			final String[] args,
			final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders ) {

		final Map<String, AvroFormatPlugin<?, ?>> stageToKafkaPlugins = new HashMap<String, AvroFormatPlugin<?, ?>>();
		for (final IngestFormatPluginProviderSpi<?, ?> pluginProvider : pluginProviders) {
			AvroFormatPlugin<?, ?> stageToKafkaPlugin = null;
			try {
				stageToKafkaPlugin = pluginProvider.getAvroFormatPlugin();

				if (stageToKafkaPlugin == null) {
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
			stageToKafkaPlugins.put(
					pluginProvider.getIngestFormatName(),
					stageToKafkaPlugin);
		}

		try {
			final StageKafkaData<T> runData = new StageKafkaData<T>(
					kafkaOptions.getProperties());
			processInput(
					stageToKafkaPlugins,
					runData);
			runData.close();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to process input",
					e);
		}

	}
}
