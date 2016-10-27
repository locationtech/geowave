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

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * 
 * Represents a set of configurations maintained within the user data of a
 * simple feature type.
 * 
 */
public class SimpleFeatureUserDataConfigurationSet
{

	private static Logger LOGGER = Logger.getLogger(SimpleFeatureUserDataConfigurationSet.class);
	public static final String SIMPLE_FEATURE_CONFIG_FILE_PROP = "SIMPLE_FEATURE_CONFIG_FILE";

	private Map<String, List<SimpleFeatureUserDataConfiguration>> configurations = new HashMap<String, List<SimpleFeatureUserDataConfiguration>>();

	public SimpleFeatureUserDataConfigurationSet() {}

	public SimpleFeatureUserDataConfigurationSet(
			final SimpleFeatureType type,
			final List<SimpleFeatureUserDataConfiguration> configurations ) {
		super();
		this.getConfigurationsForType(
				type.getTypeName()).addAll(
				configurations);
		configureFromType(type);
	}

	public Map<String, List<SimpleFeatureUserDataConfiguration>> getConfigurations() {
		return configurations;
	}

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

	public void addConfigurations(
			String typeName,
			final SimpleFeatureUserDataConfiguration config ) {
		getConfigurationsForType(
				typeName).add(
				config);
	}

	public SimpleFeatureUserDataConfigurationSet(
			final SimpleFeatureType type ) {
		for (final SimpleFeatureUserDataConfiguration configuration : this.getConfigurationsForType(type.getTypeName())) {
			configuration.configureFromType(type);
		}
	}

	public void configureFromType(
			final SimpleFeatureType type ) {
		for (final SimpleFeatureUserDataConfiguration configuration : this.getConfigurationsForType(type.getTypeName())) {
			configuration.configureFromType(type);
		}
	}

	public void updateType(
			final SimpleFeatureType type ) {
		for (final SimpleFeatureUserDataConfiguration configuration : this.getConfigurationsForType(type.getTypeName())) {
			configuration.updateType(type);
		}
	}

	@SuppressWarnings("deprecation")
	public String asJsonString()
			throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final SerializationConfig serializationConfig = mapper.getSerializationConfig();
		serializationConfig.disable(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS);
		return mapper.writeValueAsString(this);
	}

	@SuppressWarnings("deprecation")
	public void fromJsonString(
			final String jsonConfigString,
			final SimpleFeatureType type )
			throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final SerializationConfig serializationConfig = mapper.getSerializationConfig();
		serializationConfig.disable(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS);
		final SimpleFeatureUserDataConfigurationSet instance = mapper.readValue(
				jsonConfigString,
				SimpleFeatureUserDataConfigurationSet.class);
		configurations = instance.configurations;
		updateType(type);
	}

	@SuppressWarnings("deprecation")
	public static SimpleFeatureType configureType(
			final SimpleFeatureType type ) {
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
					LOGGER.error(
							"Cannot parse JSON congiguration file " + configFileName,
							e);
				}
			}
		}
		return type;

	}
}
