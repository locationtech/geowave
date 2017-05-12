package mil.nga.giat.geowave.core.cli.operations.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.Constants;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.converters.PasswordConverter;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;

@GeowaveOperation(name = "set", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Set property name within cache")
public class SetCommand extends
		DefaultOperation implements
		Command
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SetCommand.class);

	@Parameter(description = "<name> <value>")
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = {
		"--password"
	}, description = "boolean (true|false) - specify if the value being set is a password and should be encrypted in the configurations")
	private String password = null;

	private boolean isPassword;

	@Override
	public boolean prepare(
			OperationParams params ) {
		super.prepare(params);
		if (password != null && !"".equals(password.trim())) {
			isPassword = Boolean.parseBoolean(password.trim());
		}
		return true;
	}

	@Override
	public void execute(
			OperationParams params ) {

		Properties existingProps = getGeoWaveConfigProperties(params);

		PasswordConverter converter = new PasswordConverter(
				null);

		String key = null;
		String value = null;

		if (parameters.size() == 1 && parameters.get(
				0).indexOf(
				"=") != -1) {
			String[] parts = StringUtils.split(
					parameters.get(0),
					"=");
			key = parts[0];
			value = converter.convert(parts[1]);
		}
		else if (parameters.size() == 2) {
			key = parameters.get(0);
			value = converter.convert(parameters.get(1));
		}
		else {
			throw new ParameterException(
					"Requires: <name> <value>");
		}

		if (isPassword) {
			// check if encryption is enabled in configuration
			if (Boolean.parseBoolean(existingProps.getProperty(
					Constants.ENCRYPTION_ENABLED_KEY,
					"true"))) {
				try {
					File tokenFile = SecurityUtils.getFormattedTokenKeyFileForConfig(getGeoWaveConfigFile());
					value = SecurityUtils.encryptAndHexEncodeValue(
							value,
							tokenFile.getAbsolutePath());
					LOGGER.debug("Value was successfully encrypted");
				}
				catch (Exception e) {
					LOGGER.error(
							"An error occurred encrypting the specified value: " + e.getLocalizedMessage(),
							e);
				}
			}
			else {
				LOGGER.warn(
						"Value was set as a password, though encryption is currently disabled, so value was not encrypted. "
								+ "Please enable encryption and re-try.\n"
								+ "Note: To enable encryption, run the following command: geowave config set {}=true",
						Constants.ENCRYPTION_ENABLED_KEY);
			}
		}
		existingProps.setProperty(
				key,
				value);
		ConfigOptions.writeProperties(
				getGeoWaveConfigFile(params),
				existingProps);
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String key,
			String value ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(key);
		this.parameters.add(value);
	}
}