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
	private final static String KAFKA_PROPS_KEY = "kafkaprops";
	private final Properties properties;

	public KafkaCommandLineOptions(
			final Properties properties ) {
		this.properties = properties;
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option propertiesOption = new Option(
				KAFKA_PROPS_KEY,
				true,
				"Properties file containing Kafka properties");
		propertiesOption.setRequired(false);

		allOptions.addOption(propertiesOption);
	}

	protected static void applyAdditionalOptions(
			final Options allOptions,
			final KafkaCommandLineArgument[] arguments ) {
		for (final KafkaCommandLineArgument arg : arguments) {
			final Option additionalOption = new Option(
					arg.getArgName(),
					true,
					arg.getArgDescription());
			allOptions.addOption(additionalOption);
		}
	}

	public Properties getProperties() {
		return properties;
	}

	public static KafkaCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		return new KafkaCommandLineOptions(
				getBaseProperties(commandLine));
	}

	public static Properties getBaseProperties(
			final CommandLine commandLine )
			throws ParseException {
		final String kafkaPropertiesPath = commandLine.getOptionValue(KAFKA_PROPS_KEY);
		final Properties properties = new Properties();
		if (kafkaPropertiesPath != null) {
			readAndVerifyProperties(
					kafkaPropertiesPath,
					properties);
		}
		return properties;
	}

	protected static KafkaCommandLineOptions parseOptionsWithAdditionalArguments(
			final CommandLine commandLine,
			final KafkaCommandLineArgument[] additionalArguments )
			throws ParseException {
		final Properties properties = getBaseProperties(commandLine);
		for (final KafkaCommandLineArgument arg : additionalArguments) {
			if (commandLine.hasOption(arg.getArgName())) {
				final String value = commandLine.getOptionValue(arg.getArgName());
				if ((value != null) && !value.trim().isEmpty()) {
					properties.put(
							arg.getKafkaParamName(),
							value);
				}
			}
		}
		boolean success = true;
		for (final KafkaCommandLineArgument arg : additionalArguments) {
			if (arg.isRequired() && !properties.containsKey(arg.getKafkaParamName())) {
				LOGGER.fatal("Option '" + arg.getArgName() + "' must be provided or kafka properties file must contain '" + arg.getKafkaParamName() + "' property");
				success = false;
			}
		}
		if (!success) {
			throw new ParseException(
					"Required option is missing");
		}
		return new KafkaCommandLineOptions(
				properties);
	}

	private static boolean readAndVerifyProperties(
			final String kafkaPropertiesPath,
			final Properties properties ) {
		try {
			final File propFile = new File(
					kafkaPropertiesPath);
			if (!propFile.exists()) {
				LOGGER.fatal("File does not exist: " + kafkaPropertiesPath);
				return false;
			}
			final InputStreamReader inputStreamReader = new InputStreamReader(
					new FileInputStream(
							propFile),
					"UTF-8");
			properties.load(inputStreamReader);

			inputStreamReader.close();
		}
		catch (final FileNotFoundException e) {
			LOGGER.fatal(
					"Kafka properties file not found: ",
					e);
			return false;
		}
		catch (final IOException e) {
			LOGGER.fatal(
					"Unable to load Kafka properties file: ",
					e);
			return false;
		}

		return true;
	}
}
