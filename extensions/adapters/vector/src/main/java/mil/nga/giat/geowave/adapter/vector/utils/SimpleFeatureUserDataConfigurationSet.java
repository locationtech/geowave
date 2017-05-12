package mil.nga.giat.geowave.adapter.vector.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * 
 * Represents a set of configurations maintained within the user data of a
 * simple feature type and is tracked by the type name.
 * 
 */

public class SimpleFeatureUserDataConfigurationSet
{

	private static Logger LOGGER = LoggerFactory.getLogger(SimpleFeatureUserDataConfigurationSet.class);
	public static final String SIMPLE_FEATURE_CONFIG_FILE_PROP = "SIMPLE_FEATURE_CONFIG_FILE";

	/**
	 * Name string accessed Map of SimpleFeatureUserDataConfiguration in this
	 * object. The name is the SimpleFeatureType name that will have a
	 * configuration set.
	 */
	private Map<String, List<SimpleFeatureUserDataConfiguration>> configurations = new HashMap<String, List<SimpleFeatureUserDataConfiguration>>();

	/**
	 * Default Constructor<br>
	 * 
	 */
	public SimpleFeatureUserDataConfigurationSet() {}

	/**
	 * Constructor<br>
	 * Creates a new SimpleFeatureUserDataConfigurationSet configured using the
	 * passed in SimpleFeature type. Will be accessed using the type name.
	 * 
	 * @param type
	 *            - SFT to be configured
	 */
	public SimpleFeatureUserDataConfigurationSet(
			final SimpleFeatureType type ) {
		List<SimpleFeatureUserDataConfiguration> sfudc = getConfigurationsForType(type.getTypeName());

		for (final SimpleFeatureUserDataConfiguration configuration : sfudc) {
			configuration.configureFromType(type);
		}
	}

	/**
	 * Constructor<br>
	 * Creates a new SimpleFeatureUserDataConfigurationSet configured using the
	 * passed in SimpleFeature type and adding the passed in configurations.
	 * Will be accessed using the type name.
	 * 
	 * @param type
	 * @param configurations
	 */
	public SimpleFeatureUserDataConfigurationSet(
			final SimpleFeatureType type,
			final List<SimpleFeatureUserDataConfiguration> configurations ) {
		super();
		getConfigurationsForType(
				type.getTypeName()).addAll(
				configurations);
		configureFromType(type);
	}

	/**
	 * 
	 * @return a Map of all the SimpleFeatureUserDataConfiguration's by name
	 */
	public Map<String, List<SimpleFeatureUserDataConfiguration>> getConfigurations() {
		return configurations;
	}

	/**
	 * Gets a List of all the SimpleFeatureUserDataConfigurations for the SFT
	 * specified by the 'typeName' string
	 * 
	 * @param typeName
	 *            - SFT configuration desired
	 * @return - List<SimpleFeatureUserDataConfigurations>
	 */
	public synchronized List<SimpleFeatureUserDataConfiguration> getConfigurationsForType(
			String typeName ) {
		List<SimpleFeatureUserDataConfiguration> configList = configurations.get(typeName);

		if (configList == null) {
			configList = new ArrayList<SimpleFeatureUserDataConfiguration>();
			configurations.put(
					typeName,
					configList);
		}

		return configList;
	}

	/**
	 * Add the passed in configuration to the list of configurations for the
	 * specified type name
	 * 
	 * @param typeName
	 *            - name of type which will get an added configuration
	 * @param config
	 *            - configuration to be added
	 */
	public void addConfigurations(
			String typeName,
			final SimpleFeatureUserDataConfiguration config ) {
		getConfigurationsForType(
				typeName).add(
				config);
	}

	/**
	 * Updates the entire list of SimpleFeatureUserDataConfiguration(s) with
	 * information from the passed in SF type
	 * 
	 * @param type
	 *            - SF type to be updated
	 */
	public void configureFromType(
			final SimpleFeatureType type ) {
		List<SimpleFeatureUserDataConfiguration> sfudc = getConfigurationsForType(type.getTypeName());

		// Go through list of SFUD configurations and update each one with
		// information from the
		// passed in SF type

		for (final SimpleFeatureUserDataConfiguration configuration : sfudc) {
			configuration.configureFromType(type);
		}
	}

	/**
	 * Updates the SFT with the entire list of
	 * SimpleFeatureUserDataConfiguration(s)
	 * 
	 * @param type
	 *            - SF type to be updated
	 */
	public void updateType(
			final SimpleFeatureType type ) {
		List<SimpleFeatureUserDataConfiguration> sfudc = getConfigurationsForType(type.getTypeName());

		// Go through list of SFUD configurations and update each one in the
		// passed in SF type

		for (final SimpleFeatureUserDataConfiguration configuration : sfudc) {
			configuration.updateType(type);
		}
	}

	/**
	 * 
	 * @return JSON formatted string representing the
	 *         SimpleFeatureUserDataConfigurationSet represented by this object
	 * @throws JsonMappingException
	 * @throws JsonGenerationException
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public String asJsonString()
			throws JsonGenerationException,
			JsonMappingException,
			IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final SerializationConfig serializationConfig = mapper.getSerializationConfig();

		serializationConfig.disable(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS);

		return mapper.writeValueAsString(this);
	}

	/**
	 * Converts a JSON formatted config string to a
	 * SimpleFeatureUserDataConfiguationSet and updates the passed in SFT
	 * 
	 * @param jsonConfigString
	 *            - json formatted configuration string
	 * @param type
	 *            - SFT to be updated from the JSON String
	 *
	 * @throws JsonMappingException
	 * @throws JsonParseException
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public void fromJsonString(
			final String jsonConfigString,
			final SimpleFeatureType type )
			throws JsonParseException,
			JsonMappingException,
			IOException {
		final ObjectMapper mapper = new ObjectMapper();

		final SerializationConfig serializationConfig = mapper.getSerializationConfig();
		serializationConfig.disable(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS);

		final SimpleFeatureUserDataConfigurationSet instance = mapper.readValue(
				jsonConfigString,
				SimpleFeatureUserDataConfigurationSet.class);

		configurations = instance.configurations;
		updateType(type);
	}

	/**
	 * Method that reads user data configuration information from
	 * {@value #SIMPLE_FEATURE_CONFIG_FILE_PROP} and updates the passed in SFT.
	 * 
	 * @param type
	 *            - SFT to be updated
	 * 
	 * @return the SFT passed in as a parameter
	 */
	@SuppressWarnings("deprecation")
	public static SimpleFeatureType configureType(
			final SimpleFeatureType type ) {
		// HP Fortify "Path Manipulation" false positive
		// What Fortify considers "user input" comes only
		// from users with OS-level access anyway
		final String configFileName = System.getProperty(SIMPLE_FEATURE_CONFIG_FILE_PROP);
		if (configFileName != null) {
			final File configFile = new File(
					configFileName);
			if (configFile.exists() && configFile.canRead()) {
				try (FileInputStream input = new FileInputStream(
						configFile); Reader reader = new InputStreamReader(
						input,
						"UTF-8")) {
					final ObjectMapper mapper = new ObjectMapper();
					final SerializationConfig serializationConfig = mapper.getSerializationConfig();
					serializationConfig.disable(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS);
					final SimpleFeatureUserDataConfigurationSet instance = mapper.readValue(
							reader,
							SimpleFeatureUserDataConfigurationSet.class);
					instance.updateType(type);
				}
				catch (final IOException e) {
					// HP Fortify "Log Forging" false positive
					// What Fortify considers "user input" comes only
					// from users with OS-level access anyway
					LOGGER.error(
							"Cannot parse JSON congiguration file " + configFileName,
							e);
				}
			}
		}
		return type;

	}
}
