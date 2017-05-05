package mil.nga.giat.geowave.cli.geoserver;

import static mil.nga.giat.geowave.cli.geoserver.constants.GeoServerConstants.GEOSERVER_NAMESPACE_PREFIX;
import static mil.nga.giat.geowave.cli.geoserver.constants.GeoServerConstants.GEOSERVER_PASS;
import static mil.nga.giat.geowave.cli.geoserver.constants.GeoServerConstants.GEOSERVER_URL;
import static mil.nga.giat.geowave.cli.geoserver.constants.GeoServerConstants.GEOSERVER_USER;
import static mil.nga.giat.geowave.cli.geoserver.constants.GeoServerConstants.GEOSERVER_WORKSPACE;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.converters.GeoWaveBaseConverter;
import mil.nga.giat.geowave.core.cli.converters.OptionalPasswordConverter;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslator;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderTranslationMap;
import mil.nga.giat.geowave.core.cli.prefix.TranslationEntry;

@GeowaveOperation(name = "geoserver", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Create a local configuration for GeoServer")
public class ConfigGeoServerCommand extends
		DefaultOperation implements
		Command
{
	@Parameter(names = {
		"-u",
		"--url"
	}, description = "GeoServer URL (for example http://localhost:8080/geoserver or https://localhost:8443/geoserver), or simply host:port and appropriate assumptions are made")
	private String url;

	@Parameter(names = {
		"-n",
		"--name"
	}, description = "GeoServer User")
	private String name;

	// GEOWAVE-811 - adding additional password options for added protection
	@Parameter(names = {
		"-p",
		"--pass"
	}, description = "GeoServer Password - " + OptionalPasswordConverter.DEFAULT_PASSWORD_DESCRIPTION, converter = OptionalPasswordConverter.class)
	private String pass;

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, description = "GeoServer Default Workspace")
	private String workspace;

	@ParametersDelegate
	private GeoServerSSLConfigurationOptions sslConfigOptions = new GeoServerSSLConfigurationOptions();

	@Override
	public boolean prepare(
			OperationParams params ) {
		boolean retval = true;
		retval |= super.prepare(params);

		String username = getName();
		String password = getPass();

		boolean usernameSpecified = username != null && !"".equals(username.trim());
		boolean passwordSpecified = password != null && !"".equals(password.trim());
		if (usernameSpecified || passwordSpecified) {
			if (usernameSpecified && !passwordSpecified) {
				setPass(GeoWaveBaseConverter.promptAndReadPassword("Please enter a password for username [" + username
						+ "]: "));
				if (getPass() == null || "".equals(getPass().trim())) {
					throw new ParameterException(
							"Password cannot be null or empty if username is specified");
				}
			}
			else if (passwordSpecified && !usernameSpecified) {
				setName(GeoWaveBaseConverter
						.promptAndReadValue("Please enter a username associated with specified password: "));
				if (getName() == null || "".equals(getName().trim())) {
					throw new ParameterException(
							"Username cannot be null or empty if password is specified");
				}
			}
		}

		return retval;
	}

	@Override
	public void execute(
			OperationParams params )
			throws Exception {

		Properties existingProps = getGeoWaveConfigProperties(params);

		// all switches are optional
		if (url != null) {
			existingProps.setProperty(
					GEOSERVER_URL,
					url);
		}

		if (getName() != null) {
			existingProps.setProperty(
					GEOSERVER_USER,
					getName());
		}

		if (getPass() != null) {
			existingProps.setProperty(
					GEOSERVER_PASS,
					getPass());
		}

		if (getWorkspace() != null) {
			existingProps.setProperty(
					GEOSERVER_WORKSPACE,
					getWorkspace());
		}

		// save properties from ssl configurations
		sslConfigOptions.saveProperties(existingProps);

		// Write properties file
		ConfigOptions.writeProperties(
				getGeoWaveConfigFile(params),
				existingProps,
				this.getClass(),
				GEOSERVER_NAMESPACE_PREFIX);
	}

	public String getName() {
		return name;
	}

	public void setName(
			String name ) {
		this.name = name;
	}

	public String getPass() {
		return pass;
	}

	public void setPass(
			String pass ) {
		this.pass = pass;
	}

	public String getWorkspace() {
		return workspace;
	}

	public void setWorkspace(
			String workspace ) {
		this.workspace = workspace;
	}

	public GeoServerSSLConfigurationOptions getGeoServerSSLConfigurationOptions() {
		return sslConfigOptions;
	}

	public void setGeoServerSSLConfigurationOptions(
			final GeoServerSSLConfigurationOptions sslConfigOptions ) {
		this.sslConfigOptions = sslConfigOptions;
	}

	@Override
	public String usage() {
		StringBuilder builder = new StringBuilder();

		List<String> nameArray = new ArrayList<String>();
		JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
		translator.addObject(this);
		JCommanderTranslationMap map = translator.translate();
		map.createFacadeObjects();

		// Copy default parameters over for help display.
		map.transformToFacade();

		JCommander jc = new JCommander();

		Map<String, TranslationEntry> translations = map.getEntries();
		for (Object obj : map.getObjects()) {
			for (Field field : obj.getClass().getDeclaredFields()) {
				TranslationEntry tEntry = translations.get(field.getName());
				if (tEntry != null && tEntry.getObject() instanceof ConfigGeoServerCommand) {
					jc.addObject(obj);
					break;
				}
			}
		}

		String programName = StringUtils.join(
				nameArray,
				" ");
		jc.setProgramName(programName);
		jc.usage(builder);

		// Trim excess newlines.
		String operations = builder.toString().trim();

		builder = new StringBuilder();
		builder.append(operations);
		builder.append("\n\n");
		builder.append("  ");

		jc = new JCommander();

		for (Object obj : map.getObjects()) {
			for (Field field : obj.getClass().getDeclaredFields()) {
				TranslationEntry tEntry = translations.get(field.getName());
				if (tEntry != null && !(tEntry.getObject() instanceof ConfigGeoServerCommand)) {
					Parameters parameters = tEntry.getObject().getClass().getAnnotation(
							Parameters.class);
					if (parameters != null) {
						builder.append(parameters.commandDescription());
					}
					else {
						builder.append("Additional Parameters");
					}
					jc.addObject(obj);
					break;
				}
			}
		}

		jc.setProgramName(programName);
		jc.usage(builder);
		builder.append("\n\n");

		return builder.toString().trim();
	}
}
