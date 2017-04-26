package mil.nga.giat.geowave.core.ingest.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslator;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderTranslationMap;
import mil.nga.giat.geowave.core.cli.prefix.TranslationEntry;

public class KafkaCommandLineOptions
{
	private final static Logger LOGGER = LoggerFactory.getLogger(KafkaCommandLineOptions.class);

	@Parameter(names = "--kafkaprops", required = true, description = "Properties file containing Kafka properties")
	private String kafkaPropertyFile;

	// After initProperties()
	private Properties kafkaProperties = null;

	public KafkaCommandLineOptions() {}

	public Properties getProperties() {
		initProperties();
		return kafkaProperties;
	}

	public synchronized void initProperties() {
		if (kafkaProperties == null) {
			final Properties properties = new Properties();
			if (kafkaPropertyFile != null) {
				if (!readAndVerifyProperties(
						kafkaPropertyFile,
						properties)) {
					throw new ParameterException(
							"Unable to read properties file");
				}
			}
			applyOverrides(properties);
			kafkaProperties = properties;
		}
	}

	/**
	 * This function looks as 'this' and checks for @PropertyReference
	 * annotations, and overrides the string values into the props list based on
	 * the propety name in the annotation value.
	 */
	private void applyOverrides(
			Properties properties ) {
		// Get the parameters specified in this object.
		JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
		translator.addObject(this);
		JCommanderTranslationMap map = translator.translate();

		// Find objects with the PropertyReference annotation
		for (TranslationEntry entry : map.getEntries().values()) {
			if (entry.hasValue()) {
				PropertyReference ref = entry.getMember().getAnnotation(
						PropertyReference.class);
				if (ref != null) {
					String propKey = ref.value();
					String propStringValue = entry.getParam().get(
							entry.getObject()).toString();
					properties.setProperty(
							propKey,
							propStringValue);
				}
			}
		}
	}

	private static boolean readAndVerifyProperties(
			final String kafkaPropertiesPath,
			final Properties properties ) {

		final File propFile = new File(
				kafkaPropertiesPath);
		if (!propFile.exists()) {
			LOGGER.error("File does not exist: " + kafkaPropertiesPath);
			return false;
		}

		try (final FileInputStream fileInputStream = new FileInputStream(
				propFile); final InputStreamReader inputStreamReader = new InputStreamReader(
				fileInputStream,
				"UTF-8")) {
			properties.load(inputStreamReader);

			inputStreamReader.close();

		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to load Kafka properties file: ",
					e);
			return false;
		}

		return true;
	}

	/**
	 * Find bugs complained, so I added synchronized.
	 * 
	 * @param kafkaPropertyFile
	 */
	public synchronized void setKafkaPropertyFile(
			String kafkaPropertyFile ) {
		this.kafkaPropertyFile = kafkaPropertyFile;
	}
}
