package mil.nga.giat.geowave.cli.geoserver;

import java.util.Properties;

import static mil.nga.giat.geowave.cli.geoserver.constants.GeoServerConstants.*;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.converters.OptionalPasswordConverter;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

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
	}, description = "GeoServer Password - can be specified as 'pass:<password>', 'file:<local file containing the password>', "
			+ "'propfile:<local properties file containing the password>:<property file key>', 'env:<variable containing the pass>', or stdin", converter = OptionalPasswordConverter.class)
	private String pass;

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, description = "GeoServer Default Workspace")
	private String workspace;

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
}
