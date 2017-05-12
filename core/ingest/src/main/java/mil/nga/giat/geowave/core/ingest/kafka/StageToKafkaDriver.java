package mil.nga.giat.geowave.core.ingest.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.local.AbstractLocalFileDriver;
import mil.nga.giat.geowave.core.ingest.local.LocalInputCommandLineOptions;

/**
 * This class actually executes the staging of data to a Kafka topic based on
 * the available type plugin providers that are discovered through SPI.
 */
public class StageToKafkaDriver<T extends SpecificRecordBase> extends
		AbstractLocalFileDriver<AvroFormatPlugin<?, ?>, StageKafkaData<?>>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(StageToKafkaDriver.class);

	private final Map<String, AvroFormatPlugin<?, ?>> ingestPlugins;
	private final KafkaProducerCommandLineOptions kafkaOptions;

	public StageToKafkaDriver(
			KafkaProducerCommandLineOptions kafkaOptions,
			Map<String, AvroFormatPlugin<?, ?>> ingestPlugins,
			LocalInputCommandLineOptions localOptions ) {
		super(
				localOptions);
		this.kafkaOptions = kafkaOptions;
		this.ingestPlugins = ingestPlugins;
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
			LOGGER.info(
					"Unable to send file [" + file.getAbsolutePath() + "] to Kafka topic: " + e.getMessage(),
					e);
		}
	}

	public boolean runOperation(
			String inputPath ) {

		final Map<String, AvroFormatPlugin<?, ?>> stageToKafkaPlugins = ingestPlugins;

		try {
			final StageKafkaData<T> runData = new StageKafkaData<T>(
					kafkaOptions.getProperties());
			processInput(
					inputPath,
					stageToKafkaPlugins,
					runData);
			runData.close();
			return true;
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to process input",
					e);
			return false;
		}

	}
}
