package mil.nga.giat.geowave.core.store.operations.config;

import java.lang.reflect.Field;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;
import mil.nga.giat.geowave.core.cli.utils.JCommanderParameterUtils;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

@GeowaveOperation(name = "addstore", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Create a store within Geowave")
public class AddStoreCommand extends
		DefaultOperation implements
		Command
{

	private final static Logger LOGGER = LoggerFactory.getLogger(AddStoreCommand.class);

	public static final String PROPERTIES_CONTEXT = "properties";

	@Parameter(description = "<name>")
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = {
		"-d",
		"--default"
	}, description = "Make this the default store in all operations")
	private Boolean makeDefault;

	@Parameter(names = {
		"-t",
		"--type"
	}, required = true, description = "The type of store, such as accumulo, memory, etc")
	private String storeType;

	@ParametersDelegate
	private DataStorePluginOptions pluginOptions = new DataStorePluginOptions();

	@Override
	public boolean prepare(
			OperationParams params ) {
		super.prepare(params);

		Properties existingProps = getGeoWaveConfigProperties(params);

		// Load SPI options for the given type into pluginOptions.
		if (storeType != null) {
			pluginOptions.selectPlugin(storeType);
		}
		else {
			// Try to load the 'default' options.
			String defaultStore = existingProps.getProperty(DataStorePluginOptions.DEFAULT_PROPERTY_NAMESPACE);

			// Load the default index.
			if (defaultStore != null) {
				try {
					if (pluginOptions.load(
							existingProps,
							DataStorePluginOptions.getStoreNamespace(defaultStore))) {
						// Set the required type option.
						this.storeType = pluginOptions.getType();
					}
				}
				catch (ParameterException pe) {
					LOGGER.warn(
							"Couldn't load default store: " + defaultStore,
							pe);
				}
			}
		}

		// Successfully prepared.
		return true;
	}

	@Override
	public void execute(
			OperationParams params ) {

		Properties existingProps = getGeoWaveConfigProperties(params);

		// Ensure that a name is chosen.
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Must specify store name");
		}

		// Make sure we're not already in the index.
		DataStorePluginOptions existingOptions = new DataStorePluginOptions();
		if (existingOptions.load(
				existingProps,
				getNamespace())) {
			throw new ParameterException(
					"That store already exists: " + getPluginName());
		}

		if (pluginOptions.getFactoryOptions() != null) {
			pluginOptions.getFactoryOptions().validatePluginOptions(
					existingProps);
		}

		// Save the store options.
		pluginOptions.save(
				existingProps,
				getNamespace());

		if (pluginOptions.getFactoryOptions() != null) {
			Field[] fields = pluginOptions.getFactoryOptions().getClass().getDeclaredFields();
			for (Field field : fields) {
				for (Annotation annotation : field.getAnnotations()) {
					if (annotation.annotationType() == Parameter.class) {
						Parameter parameter = (Parameter) annotation;
						if (JCommanderParameterUtils.isPassword(parameter)) {
							String storeFieldName = getNamespace() + ".opts." + field.getName();
							if (existingProps.containsKey(storeFieldName)) {
								String value = existingProps.getProperty(storeFieldName);
								String encryptedValue = value;
								try {
									encryptedValue = new SecurityUtils().encryptAndHexEncodeValue(value);
								}
								catch (Exception e) {
									LOGGER.error(
											"An error occurred encrypting specified password value: "
													+ e.getLocalizedMessage(),
											e);
								}
								existingProps.setProperty(
										storeFieldName,
										encryptedValue);
							}
						}
					}
				}
			}
		}

		// Make default?
		if (Boolean.TRUE.equals(makeDefault)) {
			existingProps.setProperty(
					DataStorePluginOptions.DEFAULT_PROPERTY_NAMESPACE,
					getPluginName());
		}

		// Write properties file
		ConfigOptions.writeProperties(
				getGeoWaveConfigFile(),
				existingProps);
	}

	public DataStorePluginOptions getPluginOptions() {
		return pluginOptions;
	}

	public String getPluginName() {
		return parameters.get(0);
	}

	public String getNamespace() {
		return DataStorePluginOptions.getStoreNamespace(getPluginName());
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String storeName ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(storeName);
	}

	public Boolean getMakeDefault() {
		return makeDefault;
	}

	public void setMakeDefault(
			Boolean makeDefault ) {
		this.makeDefault = makeDefault;
	}

	public String getStoreType() {
		return storeType;
	}

	public void setStoreType(
			String storeType ) {
		this.storeType = storeType;
	}

	public void setPluginOptions(
			DataStorePluginOptions pluginOptions ) {
		this.pluginOptions = pluginOptions;
	}
}