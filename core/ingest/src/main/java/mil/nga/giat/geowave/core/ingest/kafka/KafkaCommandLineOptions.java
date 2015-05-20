package mil.nga.giat.geowave.core.ingest.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class KafkaCommandLineOptions
{

	private final static Logger LOGGER = Logger.getLogger(KafkaCommandLineOptions.class);
	private final static String[] kafkaProducerProperties = {
		"metadata.broker.list",
		"zookeeper.connect",
		"serializer.class"
	};

	public static final Integer DEFAULT_NUM_CONCURRENT_CONSUMERS = 10;
	public static final String MAX_MESSAGE_FETCH_SIZE = "5000000";

	// private final String kafkaTopic;
	private final String kafkaPropertiesPath;
	private final Integer kafkaNumConsumers;
	private final Integer kafkaConsumerTimeout;
	protected static Properties properties = new Properties();

	public KafkaCommandLineOptions(
			final String kafkaPropertiesPath,
			final Integer kafkaNumConsumers,
			final Integer kafkaConsumerTimeout ) {
		this.kafkaPropertiesPath = kafkaPropertiesPath;
		this.kafkaNumConsumers = kafkaNumConsumers;
		this.kafkaConsumerTimeout = kafkaConsumerTimeout;
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option propertiesOption = new Option(
				"kafkaprops",
				true,
				"Properties file containing Kafka properties");
		propertiesOption.setRequired(false);

		final Option numberOfConcurrentConsumersOption = new Option(
				"kafkanumconsumers",
				true,
				"Number of concurrent Kafka topics to ingest from, default is " + DEFAULT_NUM_CONCURRENT_CONSUMERS.toString());
		numberOfConcurrentConsumersOption.setRequired(false);

		final Option consumerTimeout = new Option(
				"kafkaconsumertimeout",
				true,
				"The time interval (in milliseconds) to stop listening to a topic after the last successful message has been received.  This option is the same as setting consumer.timeout.ms in the Kafka properties or the System properties.");
		consumerTimeout.setRequired(false);

		allOptions.addOption(propertiesOption);

	}

	public String getKafkaPropertiesPath() {
		return kafkaPropertiesPath;
	}

	public static Properties getProperties() {

		return properties;
	}

	public Integer getKafkaNumConsumers() {
		return kafkaNumConsumers;
	}

	public Integer getKafkaConsumerTimeout() {
		return kafkaConsumerTimeout;
	}

	public static KafkaCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		final String kafkaPropertiesPath = commandLine.getOptionValue("kafkaprops");
		final String strKafkaNumConsumers = commandLine.getOptionValue("kafkanumconsumers");
		final String strKafkaConsumerTimeout = commandLine.getOptionValue("kafkaconsumertimeout");

		boolean success = true;

		if (kafkaPropertiesPath == null) {
			final StringBuffer buffer = new StringBuffer();
			buffer.append("Kafka properties file not provided, will check system properties for the following: [");
			for (final String kafkaProp : kafkaProducerProperties) {
				buffer.append(kafkaProp + ",");
			}
			buffer.deleteCharAt(buffer.length() - 1);
			buffer.append("]");
			LOGGER.warn(buffer.toString());
			success = checkForKafkaProperties();
			if (success) {
				LOGGER.info("Kafka properties were found in System properties, continuing...");
			}
		}
		else {
			success = readAndVerifyProperties(kafkaPropertiesPath);
		}

		Integer kafkaNumberConsumersOption = DEFAULT_NUM_CONCURRENT_CONSUMERS;
		if (strKafkaNumConsumers != null) {
			try {
				kafkaNumberConsumersOption = Integer.parseInt(strKafkaNumConsumers);
			}
			catch (final NumberFormatException e) {
				LOGGER.error(
						"kafkanumconsumers argument format is incorrect",
						e);
			}
		}

		Integer kafkaConsumerTimeout = DEFAULT_NUM_CONCURRENT_CONSUMERS;
		if (strKafkaConsumerTimeout != null) {
			try {
				kafkaConsumerTimeout = Integer.parseInt(strKafkaConsumerTimeout);
			}
			catch (final NumberFormatException e) {
				LOGGER.error(
						"kafkaconsumertimeout argument format is incorrect",
						e);
			}
		}

		if (!success) {
			throw new ParseException(
					"Required option is missing");
		}

		return new KafkaCommandLineOptions(
				kafkaPropertiesPath,
				kafkaNumberConsumersOption,
				kafkaConsumerTimeout);
	}

	private static boolean readAndVerifyProperties(
			final String kafkaPropertiesPath ) {
		try {
			final InputStreamReader inputStreamReader = new InputStreamReader(
					new FileInputStream(
							new File(
									kafkaPropertiesPath)),
					"UTF-8");
			properties.load(inputStreamReader);

			inputStreamReader.close();
		}
		catch (final FileNotFoundException e) {
			LOGGER.fatal("Kafka properties file not found: " + e.getMessage());
			return false;
		}
		catch (final IOException e) {
			LOGGER.fatal("Unable to load Kafka properties file: " + e.getMessage());
			return false;
		}

		return true;
	}

	private static boolean checkForKafkaProperties() {
		boolean success = true;

		for (final String kafkaProp : kafkaProducerProperties) {
			final String property = System.getProperty(kafkaProp);
			if (property == null) {
				LOGGER.error("missing " + kafkaProp + " property");
				success = false;
			}
			else {
				properties.put(
						kafkaProp,
						property);
			}
		}

		return success;
	}
}
